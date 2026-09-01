// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

/// Counted membership transition used by Flink SQL `SELECT DISTINCT`.
///
/// Flink represents streaming DISTINCT as a keyed group aggregate with no aggregate calls. An
/// accumulate message makes a key visible at count one, duplicates only increment its count, and
/// the matching final retraction removes it. Retractions for absent keys are ignored, matching
/// Flink's `GroupAggHelper` behavior after cleanup or state expiry.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CountChange {
    Ignored,
    Present(i64, bool),
    Removed,
}

pub(super) fn apply_count_change(previous: Option<i64>, accumulate: bool) -> CountChange {
    match (previous, accumulate) {
        (None, false) => CountChange::Ignored,
        (None, true) => CountChange::Present(1, true),
        (Some(count), true) => CountChange::Present(count.wrapping_add(1), false),
        (Some(1), false) => CountChange::Removed,
        (Some(count), false) => CountChange::Present(count.wrapping_sub(1), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_only_on_membership_boundaries() {
        assert_eq!(apply_count_change(None, false), CountChange::Ignored);
        assert_eq!(
            apply_count_change(None, true),
            CountChange::Present(1, true)
        );
        assert_eq!(
            apply_count_change(Some(1), true),
            CountChange::Present(2, false)
        );
        assert_eq!(
            apply_count_change(Some(2), false),
            CountChange::Present(1, false)
        );
        assert_eq!(apply_count_change(Some(1), false), CountChange::Removed);
    }
}
