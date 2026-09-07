// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::group_aggregate::schema_admission;

impl LocalGroupAggregateProcessor {
    pub(crate) fn new(plan_bytes: &[u8], reservation: HostMemoryReservation) -> Result<Self> {
        let mut plan_reservation = reservation.sibling("local aggregate decoded plan");
        plan_reservation.resize(
            crate::execution_context::wire_memory::PlanMemory::scan(plan_bytes)?.decoded()?,
        )?;
        let native = proto::NativePlan::decode(plan_bytes)
            .map_err(|error| DataFusionError::Plan(format!("invalid native plan: {error}")))?;
        if !crate::supported_plan_protocol(native.protocol_version) {
            return Err(DataFusionError::Plan(format!(
                "unsupported plan protocol version {}",
                native.protocol_version
            )));
        }
        let plan = match native.root.and_then(|operator| operator.operator) {
            Some(proto::operator::Operator::LocalGroupAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "native plan root is not a local group aggregate".to_string(),
                ));
            }
        };
        Self::from_plan(plan, reservation, plan_reservation)
    }

    pub(super) fn from_plan(
        plan: proto::LocalGroupAggregate,
        reservation: HostMemoryReservation,
        plan_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        if (!plan.bounded_batch && plan.mini_batch_size == 0)
            || plan.mini_batch_size > usize::MAX as u64
        {
            return Err(DataFusionError::Plan(
                "local group aggregate requires a positive mini-batch size that fits usize"
                    .to_string(),
            ));
        }
        let mut schema_reservation =
            reservation.sibling("local aggregate planned schemas and codecs");
        schema_reservation.resize(schema_admission::planned_schemas(
            plan.input_schema.as_ref(),
            plan.output_schema.as_ref(),
            &plan.aggregate_calls,
        )?)?;
        let input_schema =
            crate::planner::arrow_schema(plan.input_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("local group aggregate requires an input schema".to_string())
            })?)?;
        let output_schema =
            crate::planner::arrow_schema(plan.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("local group aggregate requires an output schema".to_string())
            })?)?;
        if output_schema.fields().len() != plan.grouping_indices.len() + 1
            || output_schema
                .fields()
                .last()
                .is_none_or(|field| field.data_type() != &arrow::datatypes::DataType::Binary)
        {
            return Err(DataFusionError::Plan(
                "local group aggregate output must contain grouping fields and one BINARY accumulator"
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
                            "local group aggregate grouping index {index} is outside its input"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if !RowConverter::supports_fields(&grouping_fields) {
            return Err(DataFusionError::Plan(
                "local group aggregate grouping type is not supported by Arrow row encoding"
                    .to_string(),
            ));
        }
        let mut key_fields = Vec::with_capacity(plan.grouping_indices.len());
        let mut native_key_supported = true;
        for &index in &plan.grouping_indices {
            let index = index as usize;
            let field = input_schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "local group aggregate grouping index {index} is outside its input"
                ))
            })?;
            match KeyField::from_arrow_type(field.data_type()) {
                Ok(field) if native_key_supported => key_fields.push((index, field)),
                Ok(_) => {}
                Err(_) => {
                    native_key_supported = false;
                    key_fields.clear();
                }
            }
        }
        let calls = plan
            .aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        if plan.input_changelog && calls.iter().any(|call| !call.retractable) {
            return Err(DataFusionError::Plan(
                "local changelog aggregate requires retractable calls".into(),
            ));
        }
        for (index, &input) in plan.grouping_indices.iter().enumerate() {
            if output_schema.field(index).data_type()
                != input_schema.field(input as usize).data_type()
            {
                return Err(DataFusionError::Plan(
                    "local aggregate grouping input and output types must match".into(),
                ));
            }
        }
        for call in &calls {
            if let Some(index) = call.input_index {
                let field = input_schema.fields().get(index).ok_or_else(|| {
                    DataFusionError::Plan(
                        "local aggregate call input is outside its SQL schema".into(),
                    )
                })?;
                if call
                    .input_type
                    .as_ref()
                    .is_some_and(|kind| kind != field.data_type())
                {
                    return Err(DataFusionError::Plan(
                        "local aggregate call type differs from its SQL input".into(),
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
                        "local aggregate FILTER requires a BOOLEAN input".into(),
                    ));
                }
            }
        }
        let grouping_converter = RowConverter::new(grouping_fields)?;
        let retained = schema_admission::retained_workspace(
            &calls,
            Some(&grouping_converter),
            Some(&output_schema),
        )?
        .saturating_add(
            input_schema
                .fields()
                .iter()
                .map(|field| field.size())
                .sum::<usize>()
                .saturating_mul(2),
        )
        .saturating_add(
            key_fields
                .capacity()
                .saturating_mul(std::mem::size_of::<(usize, KeyField)>())
                .saturating_mul(2),
        );
        schema_reservation.resize(retained)?;
        let output_reservation = reservation.sibling("native local group aggregate output");
        let workspace = reservation.sibling("local aggregate batch workspace");
        Ok(Self {
            plan,
            calls,
            input_schema,
            output_schema,
            grouping_converter,
            key_fields,
            pending: HashMap::with_hasher(RandomState::new()),
            pending_order: Vec::new(),
            pending_elements: 0,
            invocation: InvocationState::Idle,
            native_output_schema: None,
            control_flushing: false,
            pending_reservation: reservation,
            output_reservation,
            workspace,
            _plan_reservation: plan_reservation,
            _schema_reservation: schema_reservation,
        })
    }
}
