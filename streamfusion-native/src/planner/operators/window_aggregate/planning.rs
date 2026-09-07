// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(in crate::planner::operators) fn validate_plan(
    plan: &proto::WindowAggregate,
    max_parallelism: u32,
) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "window aggregate max parallelism must be positive".to_string(),
        ));
    }
    let kind = proto::WindowKind::try_from(plan.kind)
        .map_err(|_| DataFusionError::Plan(format!("unknown window kind {}", plan.kind)))?;
    if plan.attached_window_start_index.is_some() != plan.attached_window_end_index.is_some() {
        return Err(DataFusionError::Plan(
            "attached window aggregate requires both start and end indices".to_string(),
        ));
    }
    if plan.partial_accumulator_index.is_some() != plan.partial_slice_end_index.is_some()
        || plan.partial_accumulator_index.is_some() != plan.partial_window_start_index.is_some()
    {
        return Err(DataFusionError::Plan(
            "global window partial input requires both accumulator and slice-end indices"
                .to_string(),
        ));
    }
    if plan.partial_accumulator_index.is_some()
        && (plan.processing_time
            || plan.attached_window_start_index.is_some()
            || matches!(
                kind,
                proto::WindowKind::Session
                    | proto::WindowKind::CountTumble
                    | proto::WindowKind::CountHop
            ))
    {
        return Err(DataFusionError::Plan(
            "two-phase global window input is supported only for event-time slicing windows"
                .to_string(),
        ));
    }
    match kind {
        proto::WindowKind::Tumble if plan.size_millis > 0 => {}
        proto::WindowKind::Hop
            if plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.size_millis % plan.slide_or_step_millis == 0 => {}
        proto::WindowKind::Cumulate
            if plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.size_millis % plan.slide_or_step_millis == 0 => {}
        proto::WindowKind::Session if plan.size_millis > 0 => {}
        proto::WindowKind::CountTumble
            if plan.processing_time
                && plan.size_millis > 0
                && plan.slide_or_step_millis == 0
                && plan.offset_millis == 0
                && plan.window_properties.is_empty() => {}
        proto::WindowKind::CountHop
            if plan.processing_time
                && plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.offset_millis == 0
                && plan.window_properties.is_empty() => {}
        proto::WindowKind::Tumble => {
            return Err(DataFusionError::Plan(
                "TUMBLE window size must be positive".to_string(),
            ));
        }
        proto::WindowKind::Hop => {
            return Err(DataFusionError::Plan(
                "HOP size must be an integral multiple of its positive slide".to_string(),
            ));
        }
        proto::WindowKind::Cumulate => {
            return Err(DataFusionError::Plan(
                "CUMULATE size must be an integral multiple of its positive step".to_string(),
            ));
        }
        proto::WindowKind::Session => {
            return Err(DataFusionError::Plan(
                "SESSION gap must be positive".to_string(),
            ));
        }
        proto::WindowKind::CountTumble => {
            return Err(DataFusionError::Plan(
                "count TUMBLE requires a positive size, processing time, no offset, and no time properties"
                    .to_string(),
            ));
        }
        proto::WindowKind::CountHop => {
            return Err(DataFusionError::Plan(
                "count HOP requires positive size and slide, processing time, no offset, and no time properties"
                    .to_string(),
            ));
        }
        proto::WindowKind::Unspecified => {
            return Err(DataFusionError::Plan(
                "window aggregate kind is unspecified".to_string(),
            ));
        }
    }
    if plan.window_properties.iter().any(|value| {
        !matches!(
            proto::WindowProperty::try_from(*value),
            Ok(proto::WindowProperty::Start
                | proto::WindowProperty::End
                | proto::WindowProperty::Time)
        )
    }) {
        return Err(DataFusionError::Plan(
            "window aggregate has an unknown output property".to_string(),
        ));
    }
    Ok(())
}
