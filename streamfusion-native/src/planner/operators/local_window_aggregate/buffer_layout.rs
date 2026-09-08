// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Flink RecordsWindowBuffer flush capacity, without storing or serializing RowData.
//!
//! The local aggregate's partial-record count depends on Flink's paged buffer boundaries.
//! Arrow/compressed accumulator sizes cannot substitute for those boundaries. This tracks
//! only the upstream WindowBytesMultiMap page geometry; actual native buffers use the usual
//! coarse Flink memory reservations independently. No allocator callbacks occur per row.

use datafusion::error::{DataFusionError, Result};

const INITIAL_BUCKET_BYTES: usize = 1024 * 1024;
const BUCKET_BYTES: usize = 8;

pub(super) struct BufferLayout {
    page_bytes: usize,
    max_pages: usize,
    bucket_pages: usize,
    key_offset: usize,
    value_offset: usize,
    keys: usize,
}

impl BufferLayout {
    /// `memory_bytes` is this physical Flink operator's managed-memory share, after consumer
    /// weights and operator fractions. It is not a StreamFusion deployment budget.
    pub(super) fn new(memory_bytes: usize, page_bytes: usize) -> Result<Self> {
        if !page_bytes.is_power_of_two()
            || !((4 * 1024)..=INITIAL_BUCKET_BYTES).contains(&page_bytes)
        {
            return Err(DataFusionError::Plan(
                "unsupported Flink window buffer page size".into(),
            ));
        }
        let bucket_pages = INITIAL_BUCKET_BYTES / page_bytes;
        let max_pages = memory_bytes / page_bytes;
        if max_pages < bucket_pages + 2 {
            return Err(DataFusionError::ResourcesExhausted(
                "local window buffer needs initial bucket, key and value pages".into(),
            ));
        }
        Ok(Self {
            page_bytes,
            max_pages,
            bucket_pages,
            key_offset: 0,
            value_offset: 0,
            keys: 0,
        })
    }

    /// Account a logical input record. False means flush existing partials, reset and retry
    /// this same record. The caller supplies Flink BinaryRow sizes calculated from Arrow
    /// values, not materialized BinaryRows. `new_key` includes the slice namespace.
    pub(super) fn append(
        &mut self,
        new_key: bool,
        key_fixed_bytes: usize,
        key_bytes: usize,
        input_fixed_bytes: usize,
        input_bytes: usize,
    ) -> Result<bool> {
        if key_fixed_bytes > key_bytes || input_fixed_bytes > input_bytes {
            return Err(DataFusionError::Plan(
                "Flink row fixed area exceeds its logical size".into(),
            ));
        }
        if new_key && self.keys >= self.bucket_pages * (self.page_bytes / BUCKET_BYTES) * 3 / 4 {
            // Flink allocates the complete new table before returning the old bucket pages.
            let new_pages = self.bucket_pages.checked_mul(2).ok_or_else(overflow)?;
            let peak = new_pages
                .checked_add(self.used_pages())
                .ok_or_else(overflow)?;
            if peak > self.max_pages
                || new_pages * (self.page_bytes / BUCKET_BYTES) > i32::MAX as usize
            {
                return Ok(false);
            }
            self.bucket_pages = new_pages;
        }
        let mut key_offset = self.key_offset;
        let mut value_offset = self.value_offset;
        let pointer;
        if new_key {
            // WindowKeySerializer writes the eight-byte window before the BinaryRow key.
            key_offset = key_offset.checked_add(8).ok_or_else(overflow)?;
            key_offset = self.row(key_offset, key_fixed_bytes, key_bytes)?;
            key_offset = self
                .aligned(key_offset, 4)?
                .checked_add(4)
                .ok_or_else(overflow)?;
            pointer = self.aligned(key_offset, 4)?;
            key_offset = self.row(
                pointer.checked_add(4).ok_or_else(overflow)?,
                input_fixed_bytes,
                input_bytes,
            )?;
        } else {
            pointer = self.aligned(value_offset, 4)?;
            value_offset = self.row(
                pointer.checked_add(4).ok_or_else(overflow)?,
                input_fixed_bytes,
                input_bytes,
            )?;
        }
        let pages = self
            .bucket_pages
            .checked_add(self.pages(key_offset))
            .and_then(|n| n.checked_add(self.pages(value_offset)))
            .ok_or_else(overflow)?;
        if pointer > i32::MAX as usize || pages > self.max_pages {
            return Ok(false);
        }
        self.key_offset = key_offset;
        self.value_offset = value_offset;
        self.keys += usize::from(new_key);
        Ok(true)
    }

    pub(super) fn reset(&mut self) {
        // Flink reuses its grown bucket table, while resetting both record areas.
        self.keys = 0;
        self.key_offset = 0;
        self.value_offset = 0;
    }

    fn used_pages(&self) -> usize {
        self.bucket_pages + self.pages(self.key_offset) + self.pages(self.value_offset)
    }

    fn pages(&self, offset: usize) -> usize {
        offset.div_ceil(self.page_bytes).max(1)
    }

    fn row(&self, offset: usize, fixed: usize, bytes: usize) -> Result<usize> {
        let fixed = fixed.checked_add(4).ok_or_else(overflow)?;
        self.aligned(offset, fixed)?
            .checked_add(4)
            .and_then(|offset| offset.checked_add(bytes))
            .ok_or_else(overflow)
    }

    fn aligned(&self, offset: usize, fixed: usize) -> Result<usize> {
        if fixed > self.page_bytes {
            return Err(DataFusionError::Plan(
                "Flink row fixed area exceeds one buffer page".into(),
            ));
        }
        let remaining = self.page_bytes - offset % self.page_bytes;
        if remaining < fixed {
            offset.checked_add(remaining).ok_or_else(overflow)
        } else {
            Ok(offset)
        }
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Flink window buffer geometry overflow".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_sql_generated_flink_local_pressure_boundary() {
        // LocalWindowFlinkControlContractTest obtains this boundary from the real generated
        // Flink operator: 3 MiB operator-only share, 32 KiB pages, 17 key/slice pairs,
        // BIGINT key and BIGINT/TIMESTAMP(3) input. No native allocation occurs here.
        let mut layout = BufferLayout::new(3 << 20, 32 << 10).unwrap();
        let mut rows = 0;
        while layout.append(rows < 17, 16, 16, 24, 24).unwrap() {
            rows += 1;
        }
        assert_eq!(rows, 64_529);
        layout.reset();
        let mut second = 0;
        while layout.append(second < 17, 16, 16, 24, 24).unwrap() {
            second += 1;
        }
        assert_eq!(second, rows);
    }

    #[test]
    fn matches_upstream_multimap_page_boundaries_for_variable_rows_and_table_growth() {
        // The same fixture is checked against Flink's actual WindowBytesMultiMap by
        // LocalWindowBufferLayoutOracleTest. Both passes include buffer reset/reuse.
        for line in include_str!("buffer_layout_cases.csv").lines().skip(1) {
            let case = line
                .split(',')
                .map(|field| field.parse::<usize>().unwrap())
                .collect::<Vec<_>>();
            let mut layout = BufferLayout::new(case[0] << 20, 32 << 10).unwrap();
            for _ in 0..2 {
                let mut rows = 0;
                while layout
                    .append(rows < case[1], case[4], case[5], case[6], case[7])
                    .unwrap()
                {
                    rows += 1;
                    assert!(rows < 1_000_000);
                }
                assert_eq!(rows, case[8], "upstream Flink fixture {line}");
                let buckets = layout.bucket_pages;
                layout.reset();
                assert_eq!(layout.bucket_pages, buckets);
            }
        }
    }

    #[test]
    fn rejected_row_is_retried_after_reset_and_empty_buffer_rejects_oversized_row() {
        let mut layout = BufferLayout::new(2 << 20, 32 << 10).unwrap();
        assert!(layout.append(true, 16, 16, 24, 900_000).unwrap());
        assert!(!layout.append(false, 16, 16, 24, 900_000).unwrap());
        layout.reset();
        assert!(layout.append(true, 16, 16, 24, 900_000).unwrap());
        layout.reset();
        assert!(!layout.append(true, 16, 16, 24, 2 << 20).unwrap());
        assert_eq!(layout.keys, 0);
    }
}
