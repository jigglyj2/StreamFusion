// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

use crate::proto;

pub(crate) fn create(
    window: &proto::WindowTableFunction,
    _child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    validate(window)?;
    Err(DataFusionError::NotImplemented(
        "StreamFusion Window TVF execution is not installed yet".to_string(),
    ))
}

fn validate(window: &proto::WindowTableFunction) -> Result<()> {
    let kind = proto::WindowKind::try_from(window.kind)
        .map_err(|_| DataFusionError::Plan(format!("unknown Window TVF kind {}", window.kind)))?;
    if kind == proto::WindowKind::Unspecified {
        return Err(DataFusionError::Plan(
            "Window TVF kind must be specified".to_string(),
        ));
    }
    if window.size_millis <= 0 {
        return Err(DataFusionError::Plan(
            "Window TVF size must be positive".to_string(),
        ));
    }
    match kind {
        proto::WindowKind::Tumble => {
            if window.slide_or_step_millis != 0 {
                return Err(DataFusionError::Plan(
                    "TUMBLE must not define a slide or step".to_string(),
                ));
            }
        }
        proto::WindowKind::Hop => {
            if window.slide_or_step_millis <= 0 {
                return Err(DataFusionError::Plan(
                    "HOP slide must be positive".to_string(),
                ));
            }
        }
        proto::WindowKind::Cumulate => {
            if window.slide_or_step_millis <= 0
                || window.size_millis % window.slide_or_step_millis != 0
            {
                return Err(DataFusionError::Plan(
                    "CUMULATE max size must be an integral multiple of its positive step"
                        .to_string(),
                ));
            }
        }
        proto::WindowKind::Unspecified => unreachable!(),
    }
    Ok(())
}

fn window_start(timestamp: i64, offset: i64, size: i64) -> i64 {
    timestamp - (timestamp - offset).rem_euclid(size)
}

fn assign_windows(window: &proto::WindowTableFunction, timestamp: i64) -> Vec<(i64, i64)> {
    match proto::WindowKind::try_from(window.kind).expect("validated Window TVF kind") {
        proto::WindowKind::Tumble => {
            let start = window_start(timestamp, window.offset_millis, window.size_millis);
            vec![(start, start + window.size_millis)]
        }
        proto::WindowKind::Hop => {
            let slide = window.slide_or_step_millis;
            let mut start = window_start(timestamp, window.offset_millis, slide);
            let mut windows = Vec::new();
            while start > timestamp - window.size_millis {
                windows.push((start, start + window.size_millis));
                start -= slide;
            }
            windows
        }
        proto::WindowKind::Cumulate => {
            let step = window.slide_or_step_millis;
            let start = window_start(timestamp, window.offset_millis, window.size_millis);
            let last_end = start + window.size_millis;
            let mut end = window_start(timestamp, window.offset_millis, step) + step;
            let mut windows = Vec::new();
            while end <= last_end {
                windows.push((start, end));
                end += step;
            }
            windows
        }
        proto::WindowKind::Unspecified => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(
        kind: proto::WindowKind,
        size: i64,
        slide_or_step: i64,
        offset: i64,
    ) -> proto::WindowTableFunction {
        proto::WindowTableFunction {
            input: None,
            time_attribute_index: 0,
            kind: kind.into(),
            size_millis: size,
            slide_or_step_millis: slide_or_step,
            offset_millis: offset,
        }
    }

    #[test]
    fn tumble_matches_flink_for_positive_negative_and_offset_timestamps() {
        let spec = window(proto::WindowKind::Tumble, 5_000, 0, 1_000);
        assert_eq!(assign_windows(&spec, 7_499), vec![(6_000, 11_000)]);
        assert_eq!(assign_windows(&spec, -1), vec![(-4_000, 1_000)]);
    }

    #[test]
    fn hop_emits_flinks_newest_start_first_order() {
        let spec = window(proto::WindowKind::Hop, 10_000, 4_000, 0);
        assert_eq!(
            assign_windows(&spec, 9_000),
            vec![(8_000, 18_000), (4_000, 14_000), (0, 10_000)]
        );
    }

    #[test]
    fn cumulate_emits_flinks_increasing_end_order() {
        let spec = window(proto::WindowKind::Cumulate, 10_000, 2_000, 0);
        assert_eq!(
            assign_windows(&spec, 4_500),
            vec![(0, 6_000), (0, 8_000), (0, 10_000)]
        );
    }

    #[test]
    fn rejects_invalid_cumulate_contract() {
        let spec = window(proto::WindowKind::Cumulate, 10_000, 3_000, 0);
        assert!(validate(&spec).is_err());
    }
}
