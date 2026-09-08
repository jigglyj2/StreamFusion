// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use prost::Message;

impl LocalWindowAggregateProcessor {
    pub(crate) fn new(serialized_plan: &[u8], reservation: HostMemoryReservation) -> Result<Self> {
        let mut plan_reservation = reservation.sibling("local window decoded plan");
        plan_reservation.resize(
            crate::execution_context::wire_memory::PlanMemory::scan(serialized_plan)?.decoded()?,
        )?;
        let native = proto::NativePlan::decode(serialized_plan)
            .map_err(|error| DataFusionError::Plan(format!("invalid native plan: {error}")))?;
        if !crate::supported_plan_protocol(native.protocol_version) {
            return Err(DataFusionError::Plan(format!(
                "unsupported plan protocol version {}",
                native.protocol_version
            )));
        }
        let plan = match native.root.and_then(|operator| operator.operator) {
            Some(proto::operator::Operator::LocalWindowAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "native plan root is not a local window aggregate".to_string(),
                ));
            }
        };
        Self::from_plan(plan, reservation, plan_reservation)
    }

    pub(super) fn from_plan(
        plan: proto::LocalWindowAggregate,
        reservation: HostMemoryReservation,
        plan_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        validate_plan(&plan)?;
        let mut schema_reservation = reservation.sibling("local window schemas and codecs");
        schema_reservation.resize(
            super::super::group_aggregate::schema_admission::planned_schemas(
                plan.input_schema.as_ref(),
                plan.output_schema.as_ref(),
                &plan.aggregate_calls,
            )?,
        )?;
        let input_schema =
            crate::planner::arrow_schema(plan.input_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("local window aggregate requires an input schema".to_string())
            })?)?;
        let output_schema =
            crate::planner::arrow_schema(plan.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan(
                    "local window aggregate requires an output schema".to_string(),
                )
            })?)?;
        if output_schema.fields().len() != plan.grouping_indices.len() + 3
            || output_schema.fields()[plan.grouping_indices.len()].data_type() != &DataType::Binary
            || output_schema.fields()[plan.grouping_indices.len() + 1].data_type()
                != &DataType::Int64
            || output_schema.fields()[plan.grouping_indices.len() + 2].data_type()
                != &DataType::Int64
        {
            return Err(DataFusionError::Plan(
                "local window output must contain grouping fields, BINARY accumulator, and BIGINT window bounds"
                    .to_string(),
            ));
        }
        let grouping_fields = plan
            .grouping_indices
            .iter()
            .map(|&index| {
                input_schema
                    .fields()
                    .get(index as usize)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "local window grouping index {index} is outside its input"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if !RowConverter::supports_fields(&grouping_fields) {
            return Err(DataFusionError::Plan(
                "local window grouping type is not supported by Arrow row encoding".to_string(),
            ));
        }
        let calls = plan
            .aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        if plan.input_changelog && calls.iter().any(|call| !call.retractable) {
            return Err(DataFusionError::Plan(
                "changelog local window aggregate requires retractable calls".to_string(),
            ));
        }
        for (index, &input) in plan.grouping_indices.iter().enumerate() {
            if output_schema.field(index).data_type()
                != input_schema.field(input as usize).data_type()
            {
                return Err(DataFusionError::Plan(
                    "local window aggregate grouping input and output types must match".into(),
                ));
            }
        }
        for call in &calls {
            if let Some(index) = call.input_index {
                let field = input_schema.fields().get(index).ok_or_else(|| {
                    DataFusionError::Plan(
                        "local window aggregate call input is outside its SQL schema".into(),
                    )
                })?;
                if call
                    .input_type
                    .as_ref()
                    .is_some_and(|kind| kind != field.data_type())
                {
                    return Err(DataFusionError::Plan(
                        "local window aggregate call type differs from its SQL input".into(),
                    ));
                }
            }
            if let Some(index) = call.filter_index {
                if input_schema
                    .fields()
                    .get(index)
                    .is_none_or(|field| field.data_type() != &arrow::datatypes::DataType::Boolean)
                {
                    return Err(DataFusionError::Plan(
                        "local window aggregate FILTER requires a BOOLEAN input".into(),
                    ));
                }
            }
        }
        let time_indices = if let Some(start) = plan.attached_window_start_index {
            vec![
                start,
                plan.attached_window_end_index.expect("validated bounds"),
            ]
        } else {
            vec![plan.time_attribute_index]
        };
        for index in time_indices {
            if input_schema
                .fields()
                .get(index as usize)
                .is_none_or(|field| !matches!(field.data_type(), DataType::Timestamp(_, _)))
            {
                return Err(DataFusionError::Plan(
                    "local window time requires a timestamp input column".into(),
                ));
            }
        }
        let shift_time_zone = if plan.shift_time_zone.is_empty() {
            chrono_tz::UTC
        } else {
            plan.shift_time_zone.parse::<Tz>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "invalid local window shift time zone {}: {error}",
                    plan.shift_time_zone
                ))
            })?
        };
        let grouped_compute = if plan.input_changelog {
            None
        } else {
            super::super::group_aggregate::grouped_compute::GroupedCompute::new(&calls)?
        };
        let output_reservation = reservation.sibling("native local window output");
        Ok(Self {
            plan,
            calls,
            grouped_compute,
            input_schema,
            output_schema,
            grouping_converter: RowConverter::new(grouping_fields)?,
            shift_time_zone,
            reservation,
            output_reservation,
            _plan_reservation: plan_reservation,
            _schema_reservation: schema_reservation,
        })
    }
}

fn validate_plan(plan: &proto::LocalWindowAggregate) -> Result<()> {
    let kind = proto::WindowKind::try_from(plan.kind)
        .map_err(|_| DataFusionError::Plan(format!("unknown local window kind {}", plan.kind)))?;
    if plan.attached_window_start_index.is_some() != plan.attached_window_end_index.is_some() {
        return Err(DataFusionError::Plan(
            "attached local window aggregate requires both window bounds".to_string(),
        ));
    }
    match kind {
        proto::WindowKind::Tumble if plan.size_millis > 0 => Ok(()),
        proto::WindowKind::Hop | proto::WindowKind::Cumulate
            if plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.size_millis % plan.slide_or_step_millis == 0 =>
        {
            Ok(())
        }
        _ => Err(DataFusionError::Plan(
            "two-phase local window aggregate requires a valid slicing TUMBLE, HOP, or CUMULATE window"
                .to_string(),
        )),
    }
}
