// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cmp::Ordering;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, Int8Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    decode_key_group_snapshot, KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey,
    StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

use super::top_n::compare::compare_rows;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_KEY_PREFIX: u8 = 26;
const INPUT_KIND_COLUMN: &str = "__streamfusion_input_row_kind";
const OUTPUT_KIND_COLUMN: &str = "__streamfusion_row_kind";
const OUTPUT_BATCH_ROWS: usize = 16_384;

struct PendingSort {
    unique: RecordBatch,
    order: Vec<usize>,
    counts: Vec<u64>,
    order_position: usize,
    emitted_from_current: u64,
}

impl PendingSort {
    fn retained_bytes(&self) -> usize {
        self.unique
            .get_array_memory_size()
            .saturating_add(
                self.order
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(
                self.counts
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }

    fn drained(&self) -> bool {
        self.order_position >= self.order.len()
    }
}

/// Bounded global full sort over a backend-neutral counted multiset of Arrow rows.
pub(crate) struct BoundedSortProcessor {
    plan: proto::BoundedSort,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    state: Box<dyn KeyedState>,
    row_converter: RowConverter,
    prepared_schema: Option<SchemaRef>,
    input_kind_index: Option<usize>,
    scratch: HostMemoryReservation,
    pending: Option<PendingSort>,
    drained: bool,
    state_read_batches: u64,
    state_write_batches: u64,
    rows_read: u64,
    rows_written: u64,
    invalid_retractions: u64,
    comparator_calls: u64,
    emitted_rows: u64,
}

impl BoundedSortProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native bounded sort batch scratch and output");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            reservation,
        )?);
        Self::with_state(serialized_plan, state, scratch)
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        scratch: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(serialized_plan, state, scratch)
    }

    fn with_state(
        serialized_plan: &[u8],
        state: Box<dyn KeyedState>,
        scratch: HostMemoryReservation,
    ) -> Result<Self> {
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("bounded sort plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::BoundedSort(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "bounded sort handle requires a BoundedSort root".to_string(),
                ));
            }
        };
        validate_plan(&plan)?;
        let input_schema = arrow_schema(plan.input_schema.as_ref().expect("validated schema"))?;
        let row_converter = RowConverter::new(
            input_schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        let mut output_fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            OUTPUT_KIND_COLUMN,
            DataType::Int8,
            false,
        )));
        Ok(Self {
            plan,
            input_schema,
            output_schema: Arc::new(Schema::new(output_fields)),
            state,
            row_converter,
            prepared_schema: None,
            input_kind_index: None,
            scratch,
            pending: None,
            drained: false,
            state_read_batches: 0,
            state_write_batches: 0,
            rows_read: 0,
            rows_written: 0,
            invalid_retractions: 0,
            comparator_calls: 0,
            emitted_rows: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<()> {
        if self.pending.is_some() || self.drained {
            return Err(DataFusionError::Execution(
                "bounded sort received input after terminal output started".to_string(),
            ));
        }
        self.prepare_schema(batch.schema())?;
        let visible_count = self.input_schema.fields().len();
        let base = batch
            .get_array_memory_size()
            .saturating_mul(2)
            .saturating_add(batch.num_rows().saturating_mul(192));
        self.scratch.resize(base)?;
        let result = self.process_accounted(&batch, visible_count);
        self.scratch.resize(0)?;
        result
    }

    fn process_accounted(&mut self, batch: &RecordBatch, visible_count: usize) -> Result<()> {
        let encoded = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count])?;
        let kinds = batch
            .column(self.input_kind_index.expect("prepared input schema"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("bounded sort RowKinds are not Arrow Int8".to_string())
            })?;
        let mut unique = HashMap::<Vec<u8>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows().max(1),
            RandomState::new(),
        );
        let mut row_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let bytes = encoded.row(row).data();
            let next = unique.len();
            let index = *unique.entry(state_key(bytes)).or_insert(next);
            row_indices.push(index);
        }
        if unique.is_empty() {
            return Ok(());
        }
        // Consume the hash table into insertion order so the same owned row bytes become the
        // backend key. This avoids retaining and copying a second row vector for every distinct
        // record in the batch.
        let mut ordered_keys = Vec::with_capacity(unique.len());
        ordered_keys.resize_with(unique.len(), || None);
        for (key, index) in unique {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("bounded sort unique index is dense"))
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef { key_group: 0, key })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        self.rows_read = self.rows_read.saturating_add(existing.len() as u64);
        let mut counts = existing
            .iter()
            .map(|value| value.as_ref().map(|value| decode_count(value)).transpose())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            let count = &mut counts[row_indices[row]];
            match kinds.value(row) {
                INSERT | UPDATE_AFTER => {
                    *count = count.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution("bounded sort row count overflow".to_string())
                    })?;
                }
                UPDATE_BEFORE | DELETE => {
                    if *count == 0 {
                        self.invalid_retractions = self.invalid_retractions.saturating_add(1);
                        return Err(DataFusionError::Execution("RowData not exist!".to_string()));
                    }
                    *count -= 1;
                }
                kind => {
                    return Err(DataFusionError::Execution(format!(
                        "bounded sort received invalid Flink RowKind {kind}"
                    )));
                }
            }
        }
        let mutations = keys
            .into_iter()
            .zip(counts)
            .map(|(key, count)| StateMutation {
                key: StateKey { key_group: 0, key },
                value: (count != 0).then(|| count.to_le_bytes().to_vec()),
            })
            .collect::<Vec<_>>();
        self.rows_written = self.rows_written.saturating_add(mutations.len() as u64);
        self.state.write_batch(mutations)?;
        self.state_write_batches = self.state_write_batches.saturating_add(1);
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<RecordBatch> {
        if self.drained {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        if self.pending.is_none() {
            if let Err(error) = self.prepare_output() {
                self.scratch.resize(0)?;
                return Err(error);
            }
            if self.pending.is_none() {
                self.drained = true;
                self.scratch.resize(0)?;
                return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
            }
        }

        let retained = self
            .pending
            .as_ref()
            .expect("prepared output")
            .retained_bytes();
        let indices = next_output_indices(self.pending.as_mut().expect("prepared output"))?;
        let output = self.take_output(&indices)?;
        let output_bytes = output.get_array_memory_size();
        let finished = self.pending.as_ref().expect("prepared output").drained();
        if finished {
            self.pending = None;
            self.drained = true;
            self.scratch.resize(output_bytes)?;
        } else {
            self.scratch.resize(retained.saturating_add(output_bytes))?;
        }
        self.emitted_rows = self.emitted_rows.saturating_add(output.num_rows() as u64);
        self.scratch.transfer_to_arrow(output_bytes)?;
        Ok(output)
    }

    fn prepare_output(&mut self) -> Result<()> {
        let snapshot = self.state.snapshot_key_group(0)?;
        self.scratch.resize(snapshot.len().saturating_mul(2))?;
        let entries = decode_key_group_snapshot(0, &snapshot)?;
        let mut encoded_rows = Vec::with_capacity(entries.len());
        let mut counts = Vec::with_capacity(entries.len());
        for (mut key, value) in entries {
            if key.first().copied() != Some(STATE_KEY_PREFIX) {
                return Err(DataFusionError::Execution(
                    "bounded sort state contains an unknown namespace".to_string(),
                ));
            }
            key.remove(0);
            encoded_rows.push(key);
            counts.push(decode_count(&value)?);
        }
        drop(snapshot);
        if encoded_rows.is_empty() {
            return Ok(());
        }
        let parser = self.row_converter.parser();
        let columns = self
            .row_converter
            .convert_rows(encoded_rows.iter().map(|row| parser.parse(row)))?;
        let unique = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
        let mut order = (0..unique.num_rows()).collect::<Vec<_>>();
        let mut compare_error = None;
        order.sort_by(|&left, &right| {
            self.comparator_calls = self.comparator_calls.saturating_add(1);
            match compare_rows(
                &unique,
                left,
                &unique,
                right,
                &self.plan.sort_key_indices,
                &self.plan.sort_ascending,
                &self.plan.sort_nulls_last,
            ) {
                Ok(ordering) => ordering,
                Err(error) => {
                    compare_error = Some(error);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = compare_error {
            return Err(error);
        }
        let pending = PendingSort {
            unique,
            order,
            counts,
            order_position: 0,
            emitted_from_current: 0,
        };
        self.scratch.resize(pending.retained_bytes())?;
        self.pending = Some(pending);
        Ok(())
    }

    fn take_output(&self, indices: &UInt32Array) -> Result<RecordBatch> {
        let pending = self.pending.as_ref().expect("prepared output");
        let mut output = pending
            .unique
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None))
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        output.push(Arc::new(Int8Array::from_value(INSERT, indices.len())));
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }

    pub(crate) fn statistics(&self) -> [u64; 7] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.rows_read,
            self.rows_written,
            self.invalid_retractions,
            self.comparator_calls,
            self.emitted_rows,
        ]
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.prepared_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "bounded sort input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        let visible_count = self.input_schema.fields().len();
        if schema.fields().len() != visible_count + 1
            || !self
                .input_schema
                .fields()
                .iter()
                .zip(&schema.fields()[..visible_count])
                .all(|(expected, actual)| expected.data_type() == actual.data_type())
            || schema.field(visible_count).name() != INPUT_KIND_COLUMN
            || schema.field(visible_count).data_type() != &DataType::Int8
        {
            return Err(DataFusionError::Execution(format!(
                "bounded sort Arrow input does not match its plan: expected {:?} plus RowKind, got {schema:?}",
                self.input_schema
            )));
        }
        self.input_kind_index = Some(visible_count);
        self.prepared_schema = Some(schema);
        Ok(())
    }
}

fn next_output_indices(pending: &mut PendingSort) -> Result<UInt32Array> {
    let mut indices = Vec::with_capacity(OUTPUT_BATCH_ROWS);
    while indices.len() < OUTPUT_BATCH_ROWS && pending.order_position < pending.order.len() {
        let row = pending.order[pending.order_position];
        let count = pending.counts[row];
        let available = count
            .checked_sub(pending.emitted_from_current)
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "bounded sort output cursor exceeded its row count".to_string(),
                )
            })?;
        let take_count = available.min((OUTPUT_BATCH_ROWS - indices.len()) as u64);
        let index = u32::try_from(row).map_err(|_| {
            DataFusionError::Execution("bounded sort unique row count exceeds u32".to_string())
        })?;
        indices.extend(std::iter::repeat_n(index, take_count as usize));
        pending.emitted_from_current += take_count;
        if pending.emitted_from_current == count {
            pending.order_position += 1;
            pending.emitted_from_current = 0;
        }
    }
    Ok(UInt32Array::from(indices))
}

fn state_key(row: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + row.len());
    key.push(STATE_KEY_PREFIX);
    key.extend_from_slice(row);
    key
}

fn decode_count(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        DataFusionError::Execution("bounded sort state count is not eight bytes".to_string())
    })?;
    let count = u64::from_le_bytes(bytes);
    if count == 0 {
        return Err(DataFusionError::Execution(
            "bounded sort state contains a zero count".to_string(),
        ));
    }
    Ok(count)
}

fn validate_plan(plan: &proto::BoundedSort) -> Result<()> {
    let schema = plan.input_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("bounded sort is missing its input schema".to_string())
    })?;
    if plan.sort_key_indices.is_empty()
        || plan.sort_key_indices.len() != plan.sort_ascending.len()
        || plan.sort_key_indices.len() != plan.sort_nulls_last.len()
    {
        return Err(DataFusionError::Plan(
            "bounded sort ordering arrays must be equally sized and non-empty".to_string(),
        ));
    }
    if plan
        .sort_key_indices
        .iter()
        .any(|&index| index as usize >= schema.fields.len())
    {
        return Err(DataFusionError::Plan(
            "bounded sort key index is outside the input schema".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use prost::Message;

    #[test]
    fn sorts_counted_rows_and_applies_all_retraction_kinds() {
        let mut processor = new_processor();
        processor
            .process_arrow(batch(
                &[3, 1, 2, 2, 2, 4],
                &["c", "a", "b", "b", "b", "d"],
                &[INSERT, INSERT, INSERT, INSERT, DELETE, UPDATE_AFTER],
            ))
            .unwrap();
        processor
            .process_arrow(batch(&[4], &["d"], &[UPDATE_BEFORE]))
            .unwrap();
        let output = processor.finish().unwrap();
        assert_eq!(integers(&output), vec![1, 2, 3]);
        assert_eq!(strings(&output), vec!["a", "b", "c"]);
        assert_eq!(kinds(&output), vec![INSERT; 3]);
        assert_eq!(processor.statistics()[..5], [2, 2, 5, 5, 0]);
    }

    #[test]
    fn canonical_snapshot_restores_to_an_identical_terminal_sort() {
        let mut source = new_processor();
        source
            .process_arrow(batch(&[4, 1, 3], &["d", "a", "c"], &[INSERT; 3]))
            .unwrap();
        let snapshot = source.snapshot_key_group(0).unwrap();
        let mut restored = new_processor();
        restored.restore_key_group(0, &snapshot).unwrap();
        restored
            .process_arrow(batch(&[2], &["b"], &[INSERT]))
            .unwrap();
        assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3, 4]);
    }

    #[test]
    fn canonical_state_moves_between_memory_and_rocksdb_with_batched_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "bounded sort memory source"),
        )
        .unwrap();
        memory
            .process_arrow(batch(&[3, 1], &["c", "a"], &[INSERT, INSERT]))
            .unwrap();
        assert_eq!(memory.statistics()[..4], [1, 1, 2, 2]);
        let snapshot = memory.snapshot_key_group(0).unwrap();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = BoundedSortProcessor::new_rocksdb(
            &plan(),
            0,
            0,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker.clone(), "bounded sort RocksDB scratch"),
        )
        .unwrap();
        rocks.restore_key_group(0, &snapshot).unwrap();
        assert_eq!(rocks.snapshot_key_group(0).unwrap(), snapshot);
        rocks.process_arrow(batch(&[2], &["b"], &[INSERT])).unwrap();
        assert_eq!(rocks.statistics()[..4], [1, 1, 1, 1]);
        let rocks_snapshot = rocks.snapshot_key_group(0).unwrap();

        let mut restored = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(broker, "bounded sort memory restore"),
        )
        .unwrap();
        restored.restore_key_group(0, &rocks_snapshot).unwrap();
        assert_eq!(integers(&restored.finish().unwrap()), vec![1, 2, 3]);
    }

    #[test]
    fn missing_retraction_fails_with_flinks_contract() {
        let mut processor = new_processor();
        let error = processor
            .process_arrow(batch(&[9], &["missing"], &[DELETE]))
            .unwrap_err();
        assert!(error.to_string().contains("RowData not exist!"));
        assert_eq!(processor.statistics()[4], 1);
    }

    #[test]
    fn drains_terminal_output_in_managed_batches_without_splitting_row_counts() {
        let rows = OUTPUT_BATCH_ROWS + 3_616;
        let mut processor = new_processor();
        processor
            .process_arrow(batch(
                &vec![7; rows],
                &vec!["same"; rows],
                &vec![INSERT; rows],
            ))
            .unwrap();

        let first = processor.finish().unwrap();
        let second = processor.finish().unwrap();
        let exhausted = processor.finish().unwrap();
        assert_eq!(first.num_rows(), OUTPUT_BATCH_ROWS);
        assert_eq!(second.num_rows(), 3_616);
        assert_eq!(exhausted.num_rows(), 0);
        assert!(integers(&first).iter().all(|&value| value == 7));
        assert!(integers(&second).iter().all(|&value| value == 7));
        assert_eq!(processor.statistics()[6], rows as u64);
    }

    #[test]
    fn accounts_state_scratch_and_output_and_releases_on_failure() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(broker.clone(), "bounded sort accounting"),
        )
        .unwrap();
        let empty_state = broker.reserved();
        processor
            .process_arrow(batch(&[2, 1], &["second", "first"], &[INSERT; 2]))
            .unwrap();
        assert!(broker.reserved() > empty_state);
        let output = processor.finish().unwrap();
        assert_eq!(output.num_rows(), 2);
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);

        let constrained = Arc::new(TestBroker::new(1 << 10));
        let mut processor = BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(constrained.clone(), "bounded sort constrained"),
        )
        .unwrap();
        let payload = "x".repeat(8 << 10);
        let error = processor
            .process_arrow(batch(&[1], &[payload.as_str()], &[INSERT]))
            .unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        drop(processor);
        assert_eq!(constrained.reserved(), 0);
    }

    fn new_processor() -> BoundedSortProcessor {
        BoundedSortProcessor::new(
            &plan(),
            0,
            0,
            HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "bounded sort test"),
        )
        .unwrap()
    }

    fn plan() -> Vec<u8> {
        let schema = proto::Schema {
            fields: vec![
                field(
                    "number",
                    proto::logical_type::Type::Integer(proto::EmptyType::default()),
                ),
                field(
                    "label",
                    proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                ),
            ],
        };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::BoundedSort(Box::new(
                    proto::BoundedSort {
                        input: None,
                        input_schema: Some(schema),
                        sort_key_indices: vec![0],
                        sort_ascending: vec![true],
                        sort_nulls_last: vec![false],
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn field(name: &str, r#type: proto::logical_type::Type) -> proto::Field {
        proto::Field {
            name: name.to_string(),
            r#type: Some(proto::LogicalType {
                nullable: true,
                r#type: Some(r#type),
            }),
        }
    }

    fn batch(numbers: &[i32], labels: &[&str], row_kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "number",
                Arc::new(Int32Array::from(numbers.to_vec())) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
            ),
            (
                INPUT_KIND_COLUMN,
                Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn integers(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn strings(batch: &RecordBatch) -> Vec<&str> {
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..values.len()).map(|row| values.value(row)).collect()
    }

    fn kinds(batch: &RecordBatch) -> Vec<i8> {
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .to_vec()
    }
}
