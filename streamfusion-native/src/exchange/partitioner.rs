// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

use super::KeyField;

mod routing_rows;
use routing_rows::{routing_rows, visit_key_groups};

/// One destination's lightweight selection over a shared Arrow batch.
#[derive(Debug, Clone)]
pub struct RoutedBatch {
    destination: u32,
    batch: Arc<RecordBatch>,
    rows: UInt32Array,
}

#[derive(Debug, Clone)]
pub struct KeyGroupBatch {
    key_group: u32,
    batch: Arc<RecordBatch>,
    start: usize,
    len: usize,
}

impl KeyGroupBatch {
    pub fn key_group(&self) -> u32 {
        self.key_group
    }

    pub fn materialize(&self) -> Result<RecordBatch> {
        self.materialize_projected(self.batch.num_columns())
    }

    pub(super) fn materialize_projected(&self, columns: usize) -> Result<RecordBatch> {
        if columns > self.batch.num_columns() {
            return Err(ArrowError::InvalidArgumentError(
                "exchange transport column count exceeds input schema".into(),
            ));
        }
        let batch = if columns == self.batch.num_columns() {
            self.batch.as_ref().clone()
        } else {
            self.batch.project(&(0..columns).collect::<Vec<_>>())?
        };
        Ok(batch.slice(self.start, self.len))
    }
}

impl RoutedBatch {
    pub fn destination(&self) -> u32 {
        self.destination
    }

    pub fn batch(&self) -> &Arc<RecordBatch> {
        &self.batch
    }

    pub fn rows(&self) -> &UInt32Array {
        &self.rows
    }

    /// Materializes this destination immediately before network serialization.
    ///
    /// Routing retains a zero-copy selection over the input. Contiguous destinations share Arrow
    /// slices; only scattered destinations gather values immediately before IPC serialization.
    pub fn materialize(&self) -> Result<RecordBatch> {
        materialize(&self.batch, &self.rows, self.batch.num_columns())
    }

    pub(super) fn materialize_projected(&self, columns: usize) -> Result<RecordBatch> {
        materialize(&self.batch, &self.rows, columns)
    }
}

/// Tags contiguous input runs with their stable Flink key group for rescalable recovery.
/// Grouping all occurrences of a key group would reorder records within the same network channel.
/// Runs keep Flink's per-channel FIFO while retaining zero-copy Arrow slices until IPC encoding.
pub fn route_batch_by_key_group(
    batch: RecordBatch,
    key_fields: &[(usize, KeyField)],
    max_parallelism: u32,
) -> Result<Vec<KeyGroupBatch>> {
    if max_parallelism == 0 || max_parallelism > 32_768 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Flink exchange max parallelism {max_parallelism} is outside 1..=32768"
        )));
    }
    let batch = Arc::new(batch);
    let mut runs: Vec<KeyGroupBatch> = Vec::new();
    visit_key_groups(&batch, key_fields, max_parallelism, |row, key_group| {
        if let Some(last) = runs.last_mut().filter(|last| last.key_group == key_group) {
            last.len += 1;
        } else {
            runs.push(KeyGroupBatch {
                key_group,
                batch: Arc::clone(&batch),
                start: row as usize,
                len: 1,
            });
        }
    })?;
    Ok(runs)
}

fn materialize(
    batch: &RecordBatch,
    rows: &UInt32Array,
    column_count: usize,
) -> Result<RecordBatch> {
    if column_count > batch.num_columns() {
        return Err(ArrowError::InvalidArgumentError(
            "exchange transport column count exceeds input schema".into(),
        ));
    }
    let projected = if column_count == batch.num_columns() {
        batch.clone()
    } else {
        // Drop input-only routing columns before any gather can copy their payloads.
        batch.project(&(0..column_count).collect::<Vec<_>>())?
    };
    if rows.is_empty() {
        return Ok(projected.slice(0, 0));
    }
    let start = rows.value(0) as usize;
    if start < projected.num_rows()
        && rows.len() <= projected.num_rows() - start
        && rows
            .values()
            .iter()
            .enumerate()
            .all(|(offset, &row)| row as usize == start + offset)
    {
        // Arrow IPC handles slice offsets itself. No Java-safe normalization or gather is needed
        // when the destination already owns a contiguous selection, including the whole batch.
        return Ok(projected.slice(start, rows.len()));
    }
    let columns = projected
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), rows, None))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(projected.schema(), columns)
}

/// Routes rows by Flink key group without serializing or eagerly gathering Arrow columns.
pub fn route_batch(
    batch: RecordBatch,
    key_fields: &[(usize, KeyField)],
    max_parallelism: u32,
    parallelism: u32,
) -> Result<Vec<RoutedBatch>> {
    if parallelism == 0 || parallelism > max_parallelism || max_parallelism > 32_768 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Flink exchange requires 0 < parallelism ({parallelism}) <= max parallelism ({max_parallelism}) <= 32768"
        )));
    }
    let destination_rows = routing_rows(&batch, key_fields, max_parallelism, parallelism)?;
    let batch = Arc::new(batch);
    Ok(destination_rows
        .into_iter()
        .enumerate()
        .filter(|(_, rows)| !rows.is_empty())
        .map(|(destination, rows)| RoutedBatch {
            destination: destination as u32,
            batch: Arc::clone(&batch),
            rows: UInt32Array::from(rows),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int32Array, Int8Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn routes_by_flink_key_group_and_shares_the_arrow_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![-1, 0, 1, 42]));
        let buffer = values.values().as_ptr();
        let batch = RecordBatch::try_new(schema, vec![values as ArrayRef]).unwrap();

        let routed = route_batch(batch, &[(0, KeyField::Integer)], 128, 4).unwrap();

        assert_eq!(
            routed
                .iter()
                .map(|partition| (partition.destination(), partition.rows().values().to_vec()))
                .collect::<Vec<_>>(),
            vec![(2, vec![2, 3]), (3, vec![0, 1])]
        );
        assert!(routed
            .windows(2)
            .all(|pair| Arc::ptr_eq(pair[0].batch(), pair[1].batch())));
        assert_eq!(
            routed[0]
                .batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ptr(),
            buffer
        );
    }

    #[test]
    fn gathers_each_destination_with_stable_changelog_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![-1, 0, 1, 42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["minus", "zero", "one", "forty-two"])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, 2, 0, 3])) as ArrayRef,
            ],
        )
        .unwrap();

        let routed = route_batch(batch, &[(0, KeyField::Integer)], 128, 4).unwrap();
        let destination_two = routed
            .iter()
            .find(|partition| partition.destination() == 2)
            .unwrap()
            .materialize()
            .unwrap();

        assert_eq!(
            destination_two
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["one", "forty-two"])
        );
        assert_eq!(
            destination_two
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap(),
            &Int8Array::from(vec![0, 3])
        );
    }
}

#[cfg(test)]
mod materialization_tests;

#[cfg(test)]
mod key_group_runs_tests;
