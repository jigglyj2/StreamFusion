// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(in crate::planner::operators) fn local_to_epoch(local: NaiveDateTime, zone: Tz) -> Result<i64> {
    match zone.from_local_datetime(&local) {
        LocalResult::Single(value) => Ok(value.timestamp_millis()),
        // LocalDateTime.atZone, which Flink uses for a window-time property, selects the
        // earlier offset during an overlap. Timer registration deliberately differs below.
        LocalResult::Ambiguous(left, right) => {
            Ok(left.timestamp_millis().min(right.timestamp_millis()))
        }
        LocalResult::None => {
            // Java resolves a nonexistent local timestamp by shifting it forward by the
            // transition gap. Applying the last valid pre-transition offset is equivalent.
            let mut distance = 1i64;
            while distance <= 7 * 24 * 60 * 60 * 1_000 {
                let candidate = local
                    .checked_sub_signed(chrono::TimeDelta::milliseconds(distance))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "window property overflow while resolving a DST gap".to_string(),
                        )
                    })?;
                if let LocalResult::Single(value) = zone.from_local_datetime(&candidate) {
                    let offset_millis = i64::from(value.offset().fix().local_minus_utc()) * 1_000;
                    return Ok(local.and_utc().timestamp_millis() - offset_millis);
                }
                distance = distance.saturating_mul(2);
            }
            Err(DataFusionError::Execution(format!(
                "could not resolve local window property {local} in {zone}"
            )))
        }
    }
}

pub(in crate::planner::operators) fn local_to_timer_epoch(
    local: NaiveDateTime,
    zone: Tz,
) -> Result<i64> {
    match zone.from_local_datetime(&local) {
        LocalResult::Single(value) => Ok(value.timestamp_millis()),
        LocalResult::Ambiguous(left, right) => {
            Ok(left.timestamp_millis().max(right.timestamp_millis()))
        }
        LocalResult::None => {
            // Flink registers every nonexistent local time in a DST gap at the first valid
            // instant after the gap. Find that boundary without assuming a one-hour transition.
            let mut high = 1i64;
            while high <= 7 * 24 * 60 * 60 * 1_000 {
                let candidate = local
                    .checked_add_signed(chrono::TimeDelta::milliseconds(high))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "window timer overflow while resolving a DST gap".to_string(),
                        )
                    })?;
                if !matches!(zone.from_local_datetime(&candidate), LocalResult::None) {
                    let mut low = 0i64;
                    while low + 1 < high {
                        let middle = low + (high - low) / 2;
                        let candidate = local
                            .checked_add_signed(chrono::TimeDelta::milliseconds(middle))
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "window timer overflow while resolving a DST gap".to_string(),
                                )
                            })?;
                        if matches!(zone.from_local_datetime(&candidate), LocalResult::None) {
                            low = middle;
                        } else {
                            high = middle;
                        }
                    }
                    let first_valid = local
                        .checked_add_signed(chrono::TimeDelta::milliseconds(high))
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "window timer overflow while resolving a DST gap".to_string(),
                            )
                        })?;
                    return match zone.from_local_datetime(&first_valid) {
                        LocalResult::Single(value) => Ok(value.timestamp_millis()),
                        LocalResult::Ambiguous(left, right) => {
                            Ok(left.timestamp_millis().max(right.timestamp_millis()))
                        }
                        LocalResult::None => {
                            unreachable!("binary search ended at a valid local time")
                        }
                    };
                }
                high = high.saturating_mul(2);
            }
            Err(DataFusionError::Execution(format!(
                "could not resolve local window timer {local} in {zone}"
            )))
        }
    }
}
