// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink session namespaces, independent of aggregate computation and backend I/O.
//! Process arrival order: sorting input first could resurrect an already-dropped late row.

use datafusion::error::{DataFusionError, Result};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Interval {
    pub start: i64,
    pub end: i64,
    pub slot: usize,
}

#[derive(Default)]
pub(super) struct Assignments {
    // Disjoint active intervals have identical start/end ordering. Lookup by end finds
    // the first possible overlap, including an arbitrarily long preceding session.
    intervals: BTreeMap<i64, Interval>,
    parents: Vec<usize>,
}

impl Assignments {
    pub fn existing(&mut self, start: i64, end: i64) -> Result<usize> {
        if start >= end
            || self
                .intervals
                .range(start..)
                .next()
                .is_some_and(|(_, next)| next.start <= end)
        {
            return Err(DataFusionError::Execution(
                "overlapping or invalid persisted sessions".into(),
            ));
        }
        let slot = self.parents.len();
        self.parents.push(slot);
        self.intervals.insert(end, Interval { start, end, slot });
        Ok(slot)
    }

    pub fn assign(&mut self, timestamp: i64, gap: i64, watermark: i64) -> Result<Option<usize>> {
        let mut start = timestamp;
        let mut end = timestamp
            .checked_add(gap)
            .filter(|&end| end > timestamp)
            .ok_or_else(|| {
                DataFusionError::Execution("session window end overflows TIMESTAMP(3)".into())
            })?;
        let mut slot = None;
        while let Some((&key, &overlap)) = self.intervals.range(start..).next() {
            if overlap.start > end {
                break;
            }
            self.intervals.remove(&key);
            start = start.min(overlap.start);
            end = end.max(overlap.end);
            match slot {
                None => slot = Some(overlap.slot),
                Some(target) => self.parents[overlap.slot] = target,
            }
        }
        // Existing sessions are live at this watermark. Only an isolated event can be late.
        if end - 1 <= watermark {
            if slot.is_some() {
                return Err(DataFusionError::Execution(
                    "expired session remained in active state".into(),
                ));
            }
            return Ok(None);
        }
        let slot = slot.unwrap_or_else(|| {
            let slot = self.parents.len();
            self.parents.push(slot);
            slot
        });
        self.intervals.insert(end, Interval { start, end, slot });
        Ok(Some(slot))
    }

    pub fn resolve(&mut self, slot: usize) -> usize {
        let mut root = slot;
        while self.parents[root] != root {
            root = self.parents[root];
        }
        let mut current = slot;
        while self.parents[current] != root {
            let parent = self.parents[current];
            self.parents[current] = root;
            current = parent;
        }
        root
    }

    pub fn intervals(&self) -> impl Iterator<Item = Interval> + '_ {
        self.intervals.values().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_arrival_order_and_inclusive_bridges_keep_existing_contributions() {
        let mut map = Assignments::default();
        let first = map.existing(10_000, 20_000).unwrap();
        let second = map.existing(30_000, 40_000).unwrap();
        assert_eq!(map.assign(-1, 10_000, 15_000).unwrap(), None);
        let older = map.assign(0, 10_000, 15_000).unwrap().unwrap();
        let bridge = map.assign(20_000, 10_000, 15_000).unwrap().unwrap();
        assert_eq!(map.resolve(first), map.resolve(older));
        assert_eq!(map.resolve(second), map.resolve(bridge));
        assert_eq!(map.resolve(first), map.resolve(second));
        assert_eq!(
            map.intervals().collect::<Vec<_>>(),
            vec![Interval {
                start: 0,
                end: 40_000,
                slot: map.resolve(first)
            }]
        );
    }

    #[test]
    fn many_unrelated_sessions_are_not_merged_and_full_timestamp_bounds_are_checked() {
        let mut map = Assignments::default();
        for i in 0..10_000 {
            map.existing(i * 100, i * 100 + 10).unwrap();
        }
        for i in (0..10_000).rev() {
            map.assign(i * 100 + 5, 10, -1).unwrap();
        }
        assert_eq!(map.intervals().count(), 10_000);
        assert!(map
            .intervals()
            .all(|interval| interval.end - interval.start == 15));
        assert!(map.existing(1, 2).is_err());
        assert!(map.assign(i64::MAX, 10, -1).is_err());
        assert!(map.assign(i64::MIN, 10, i64::MIN).unwrap().is_some());
    }
}
