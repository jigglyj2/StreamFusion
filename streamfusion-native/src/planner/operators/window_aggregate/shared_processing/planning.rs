// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

pub(super) fn validate(plan: &proto::WindowAggregate) -> Result<()> {
    if !plan.processing_time
        || plan.kind != proto::WindowKind::Tumble as i32
        || plan.size_millis <= 0
        || plan.input_changelog
        || plan.partial_accumulator_index.is_some()
        || plan.attached_window_end_index.is_some()
        || !(plan.shift_time_zone.is_empty() || plan.shift_time_zone == "UTC")
        || plan.aggregate_calls.is_empty()
        || plan.aggregate_calls.iter().any(|call| {
            call.function != proto::AggregateFunction::CountStar as i32
                || call.filter_index.is_some()
                || call.distinct
        })
    {
        return Err(DataFusionError::Plan(
            "buffered processing-time windows require direct append-only UTC TUMBLE COUNT(*)"
                .into(),
        ));
    }
    let field = plan
        .input_schema
        .as_ref()
        .and_then(|schema| schema.fields.get(plan.time_attribute_index as usize))
        .and_then(|field| field.r#type.as_ref());
    if !matches!(
        field.and_then(|field| field.r#type.as_ref()),
        Some(proto::logical_type::Type::TimestampLtz(
            proto::PrecisionType { precision: 3 }
        ))
    ) {
        return Err(DataFusionError::Plan(
            "processing-time input requires a TIMESTAMP_LTZ(3) clock attribute".into(),
        ));
    }
    Ok(())
}

pub(super) fn buffer_plan(plan: &proto::WindowAggregate) -> Result<proto::LocalWindowAggregate> {
    let input = plan.input_schema.as_ref().unwrap();
    let mut fields = plan
        .grouping_indices
        .iter()
        .map(|&index| {
            input.fields.get(index as usize).cloned().ok_or_else(|| {
                DataFusionError::Plan("processing-time grouping index outside its input".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for (name, data_type) in [
        (
            "__streamfusion_accumulator",
            proto::logical_type::Type::Binary(proto::EmptyType {}),
        ),
        (
            "__streamfusion_window_start",
            proto::logical_type::Type::Bigint(proto::EmptyType {}),
        ),
        (
            "__streamfusion_slice_end",
            proto::logical_type::Type::Bigint(proto::EmptyType {}),
        ),
    ] {
        fields.push(proto::Field {
            name: name.into(),
            r#type: Some(proto::LogicalType {
                nullable: false,
                r#type: Some(data_type),
            }),
        });
    }
    Ok(proto::LocalWindowAggregate {
        input: None,
        grouping_indices: plan.grouping_indices.clone(),
        aggregate_calls: plan.aggregate_calls.clone(),
        input_changelog: false,
        time_attribute_index: plan.time_attribute_index,
        kind: plan.kind,
        size_millis: plan.size_millis,
        slide_or_step_millis: plan.slide_or_step_millis,
        offset_millis: plan.offset_millis,
        shift_time_zone: plan.shift_time_zone.clone(),
        input_schema: plan.input_schema.clone(),
        output_schema: Some(proto::Schema { fields }),
        attached_window_start_index: None,
        attached_window_end_index: None,
    })
}
