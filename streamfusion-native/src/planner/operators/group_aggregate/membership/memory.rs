// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl MembershipLayout {
    pub(super) fn batch_admission(
        &self,
        batch: &RecordBatch,
        keys: &[StateKey],
        staged: &[Option<AccumulatorState>],
        external: &[bool],
    ) -> usize {
        if batch.num_rows() == 0 {
            return 0;
        }
        // One coarse reservation covers the input-sized key directory, Arrow row encodings,
        // count vectors and dirty mutations. Legacy maps need a one-time migration allowance.
        let longest_key = keys.iter().map(|key| key.key.len()).max().unwrap_or(0);
        let mut allowance = 65536usize;
        for column in &self.columns {
            allowance = allowance
                .saturating_add(
                    batch.num_rows().saturating_mul(
                        longest_key
                            .saturating_add(384)
                            // A selected argument loads every historical filter count, even
                            // for filters that reject every row in this batch. Admit these
                            // partial maps independently of new, FILTER-eligible map growth.
                            .saturating_add(column.calls.len().saturating_mul(
                                accumulator::counted_map_entry_bytes().saturating_add(32),
                            )),
                    ),
                )
                .saturating_add(
                    batch
                        .column(column.input)
                        .get_array_memory_size()
                        .saturating_mul(4),
                );
            for (group, value) in staged
                .iter()
                .enumerate()
                .filter(|(group, _)| !external[*group])
            {
                if let Some(value) = value {
                    for &call in &column.calls {
                        let Accumulator::DistinctCount { values, .. } = &value.accumulators[call]
                        else {
                            unreachable!()
                        };
                        allowance = allowance
                            .saturating_add(
                                values.len().saturating_mul(
                                    keys[group]
                                        .key
                                        .len()
                                        .saturating_add(512)
                                        .saturating_add(column.calls.len() * 32),
                                ),
                            )
                            .saturating_add(
                                values
                                    .keys()
                                    .map(|value| value.dynamic_bytes().saturating_mul(4))
                                    .sum::<usize>(),
                            );
                    }
                }
            }
        }
        allowance
    }
}
