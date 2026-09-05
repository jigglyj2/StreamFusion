// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, BinaryArray, BooleanArray, Int32Array, Int8Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::planner::operators::top_n::compare::record_equaliser_rows;
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;
const STATE_MAGIC: &[u8; 4] = b"SFNC";
const STATE_VERSION: u8 = 1;

/// Arrow-native implementation of Flink's keyed ChangelogNormalize node.
pub(crate) struct ChangelogNormalizeProcessor {
    plan: proto::ChangelogNormalize,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    visible_schema: SchemaRef,
    output_schema: SchemaRef,
    row_converter: RowConverter,
    requires_semantic_equalizer: bool,
    input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    filter_result_index: Option<usize>,
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
    expired_state_entries: u64,
}

struct StoredRow {
    expires_at: i64,
    row: Vec<u8>,
}

struct StagedValue {
    key: StateKey,
    value: Option<StoredRow>,
    touched: bool,
}

struct OutputRow {
    row: Vec<u8>,
    kind: i8,
    input_ordinal: i32,
}

impl ChangelogNormalizeProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native changelog normalize batch scratch and output");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            reservation,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state, scratch)
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        max_parallelism: u32,
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
        Self::with_state(serialized_plan, max_parallelism, state, scratch)
    }

    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
        state: Box<dyn KeyedState>,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let native_plan = decode_plan(serialized_plan)?;
        let root = native_plan.root.ok_or_else(|| {
            DataFusionError::Plan("changelog normalize plan has no root".to_string())
        })?;
        let plan = match root.operator {
            Some(proto::operator::Operator::ChangelogNormalize(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "changelog normalize handle requires a ChangelogNormalize root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let visible_schema = arrow_schema(plan.input_schema.as_ref().expect("validated schema"))?;
        let row_converter = row_converter(&visible_schema)?;
        let requires_semantic_equalizer = visible_schema
            .fields()
            .iter()
            .any(|field| requires_semantic_equality(field.data_type()));
        let mut fields = visible_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "__streamfusion_input_row",
            DataType::Int32,
            false,
        )));
        let output_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            plan,
            max_parallelism,
            state,
            visible_schema,
            output_schema,
            row_converter,
            requires_semantic_equalizer,
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            filter_result_index: None,
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            expired_state_entries: 0,
        })
    }

    pub(crate) fn process_arrow(
        &mut self,
        batch: RecordBatch,
        now_millis: i64,
    ) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema())?;
        let visible_count = self.visible_schema.fields().len();
        let input_bytes = batch.columns()[..visible_count]
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>();
        let base = input_bytes.saturating_add(batch.num_rows().saturating_mul(192));
        self.scratch_reservation.resize(base)?;
        let encoded = self
            .row_converter
            .convert_columns(&batch.columns()[..visible_count]);
        let result = match encoded {
            Ok(encoded) => self.process_accounted(&batch, &encoded, now_millis),
            Err(error) => Err(error.into()),
        };
        match result {
            Ok(output) => self.finish_output(output, base),
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_accounted(
        &mut self,
        batch: &RecordBatch,
        encoded: &arrow_row::Rows,
        now_millis: i64,
    ) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("changelog normalize RowKinds are not Int8".to_string())
            })?;
        let filters = self
            .filter_result_index
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "changelog normalize filter results are not Boolean".to_string(),
                        )
                    })
            })
            .transpose()?;

        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut row_state_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.state_key(batch, row)?;
            let key_group = assign_key_group(&key, self.max_parallelism);
            let state_key = StateKey { key_group, key };
            let next = unique.len();
            row_state_indices.push(*unique.entry(state_key).or_insert(next));
        }
        if unique.is_empty() {
            return self.empty_output();
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("changelog normalize state index is populated"))
            .collect::<Vec<_>>();
        let refs = keys
            .iter()
            .map(|key| StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&refs)?;
        self.state_read_batches = self.state_read_batches.saturating_add(1);
        let mut staged = keys
            .into_iter()
            .zip(existing)
            .map(|(key, bytes)| {
                Ok(StagedValue {
                    key,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref()))
                        .transpose()?,
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut output = Vec::with_capacity(batch.num_rows() * 2);
        for row in 0..batch.num_rows() {
            let ordinal = i32::try_from(row).map_err(|_| {
                DataFusionError::Execution(
                    "changelog normalize batch exceeds Int32 indexing".to_string(),
                )
            })?;
            let entry = &mut staged[row_state_indices[row]];
            if entry
                .value
                .as_ref()
                .is_some_and(|value| value.expires_at <= now_millis)
            {
                entry.value = None;
                entry.touched = true;
                self.expired_state_entries = self.expired_state_entries.saturating_add(1);
            }
            let current = encoded.row(row).data();
            let passes_filter = filters
                .map(|filters| !filters.is_null(row) && filters.value(row))
                .unwrap_or(true);
            match kinds.value(row) {
                INSERT | UPDATE_AFTER => match entry.value.as_ref() {
                    None if passes_filter => {
                        output.push(OutputRow {
                            row: current.to_vec(),
                            kind: INSERT,
                            input_ordinal: ordinal,
                        });
                        entry.value = Some(StoredRow {
                            expires_at: expiration(now_millis, self.plan.state_ttl_millis),
                            row: current.to_vec(),
                        });
                        entry.touched = true;
                    }
                    None => {}
                    Some(previous)
                        if self.plan.state_ttl_millis == 0
                            && self.rows_equal(batch, row, current, &previous.row)? => {}
                    Some(previous) if passes_filter => {
                        if self.plan.generate_update_before {
                            output.push(OutputRow {
                                row: previous.row.clone(),
                                kind: UPDATE_BEFORE,
                                input_ordinal: ordinal,
                            });
                        }
                        output.push(OutputRow {
                            row: current.to_vec(),
                            kind: UPDATE_AFTER,
                            input_ordinal: ordinal,
                        });
                        entry.value = Some(StoredRow {
                            expires_at: expiration(now_millis, self.plan.state_ttl_millis),
                            row: current.to_vec(),
                        });
                        entry.touched = true;
                    }
                    Some(previous) => {
                        output.push(OutputRow {
                            row: previous.row.clone(),
                            kind: DELETE,
                            input_ordinal: ordinal,
                        });
                        entry.value = None;
                        entry.touched = true;
                    }
                },
                UPDATE_BEFORE | DELETE => {
                    if let Some(previous) = entry.value.take() {
                        output.push(OutputRow {
                            row: previous.row,
                            kind: DELETE,
                            input_ordinal: ordinal,
                        });
                        entry.touched = true;
                    }
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )));
                }
            }
        }
        let mutations = staged
            .into_iter()
            .filter(|entry| entry.touched)
            .map(|entry| StateMutation {
                key: entry.key,
                value: entry.value.map(|value| encode_state(&value)),
            })
            .collect::<Vec<_>>();
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.output_batch(output)
    }

    pub(crate) fn statistics(&self) -> [u64; 3] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.expired_state_entries,
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

    fn state_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_index {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "changelog normalize preencoded keys are not Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields)?),
        }
    }

    fn rows_equal(
        &self,
        current_batch: &RecordBatch,
        current_row: usize,
        current_bytes: &[u8],
        previous_bytes: &[u8],
    ) -> Result<bool> {
        if !self.requires_semantic_equalizer {
            return Ok(current_bytes == previous_bytes);
        }
        let parser = self.row_converter.parser();
        let previous_columns = self
            .row_converter
            .convert_rows([parser.parse(previous_bytes)])?;
        let previous = RecordBatch::try_new(self.visible_schema.clone(), previous_columns)?;
        record_equaliser_rows(
            current_batch,
            current_row,
            &previous,
            0,
            0..self.visible_schema.fields().len(),
        )
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "changelog normalize input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        self.filter_result_index = metadata_index(&schema, "__streamfusion_filter_result");
        if self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "changelog normalize requires RowKind metadata".to_string(),
            ));
        }
        if self.plan.has_filter != self.filter_result_index.is_some() {
            return Err(DataFusionError::Execution(
                "changelog normalize filter metadata does not match its plan".to_string(),
            ));
        }
        let visible_count = [
            self.preencoded_key_index,
            self.input_kind_index,
            self.filter_result_index,
            Some(schema.fields().len()),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap();
        if visible_count != self.visible_schema.fields().len()
            || schema.fields()[..visible_count]
                .iter()
                .zip(self.visible_schema.fields())
                .any(|(actual, expected)| actual.data_type() != expected.data_type())
        {
            return Err(DataFusionError::Execution(
                "changelog normalize Arrow schema does not match its protobuf schema".to_string(),
            ));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .key_indices
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "changelog normalize key {index} is outside the visible row"
                        ))
                    })?;
                    Ok((
                        index as usize,
                        KeyField::from_arrow_type(field.data_type())?,
                    ))
                })
                .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;
        }
        self.input_schema = Some(schema);
        Ok(())
    }

    fn output_batch(&self, rows: Vec<OutputRow>) -> Result<RecordBatch> {
        let parser = self.row_converter.parser();
        let decoded = self
            .row_converter
            .convert_rows(rows.iter().map(|row| parser.parse(&row.row)))?;
        let mut columns = decoded;
        columns.push(Arc::new(Int8Array::from(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int32Array::from(
            rows.iter().map(|row| row.input_ordinal).collect::<Vec<_>>(),
        )));
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    fn empty_output(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(self.output_schema.clone()))
    }

    fn finish_output(&mut self, output: RecordBatch, base: usize) -> Result<RecordBatch> {
        let output_bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(output_bytes.max(base))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }
}

fn validate_plan(plan: &proto::ChangelogNormalize, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0 || plan.key_indices.is_empty() || plan.input_schema.is_none() {
        return Err(DataFusionError::Plan(
            "changelog normalize key/schema contract is invalid".to_string(),
        ));
    }
    Ok(())
}

fn expiration(now_millis: i64, ttl_millis: u64) -> i64 {
    if ttl_millis == 0 {
        i64::MAX
    } else {
        now_millis.saturating_add(i64::try_from(ttl_millis).unwrap_or(i64::MAX))
    }
}

fn encode_state(value: &StoredRow) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(17 + value.row.len());
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&value.expires_at.to_le_bytes());
    bytes.extend_from_slice(&(value.row.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&value.row);
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<StoredRow> {
    if bytes.len() < 17 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "invalid native changelog normalize state".to_string(),
        ));
    }
    let expires_at = i64::from_le_bytes(bytes[5..13].try_into().unwrap());
    let length = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
    if bytes.len() != 17usize.saturating_add(length) {
        return Err(DataFusionError::Execution(
            "native changelog normalize state length is invalid".to_string(),
        ));
    }
    Ok(StoredRow {
        expires_at,
        row: bytes[17..].to_vec(),
    })
}

fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn row_converter(schema: &SchemaRef) -> Result<RowConverter> {
    Ok(RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                SortField::new_with_options(
                    field.data_type().clone(),
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                )
            })
            .collect(),
    )?)
}

fn requires_semantic_equality(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 | DataType::Map(_, _) => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            requires_semantic_equality(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| requires_semantic_equality(field.data_type())),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use prost::Message;

    #[test]
    fn normalizes_upserts_retractions_duplicates_and_ttl() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = ChangelogNormalizeProcessor::new(
            &plan(true, 10),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "normalize test"),
        )
        .unwrap();
        let empty_state = broker.reserved();
        let first = processor
            .process_arrow(
                batch(
                    &[7, 7, 7],
                    &["a", "a", "b"],
                    &[INSERT, UPDATE_AFTER, UPDATE_AFTER],
                ),
                100,
            )
            .unwrap();
        assert!(broker.reserved() > empty_state);
        assert_eq!(
            kinds(&first),
            vec![
                INSERT,
                UPDATE_BEFORE,
                UPDATE_AFTER,
                UPDATE_BEFORE,
                UPDATE_AFTER
            ]
        );
        assert_eq!(strings(&first), vec!["a", "a", "a", "a", "b"]);

        let delete = processor
            .process_arrow(batch(&[7], &["tombstone"], &[DELETE]), 101)
            .unwrap();
        assert_eq!(kinds(&delete), vec![DELETE]);
        assert_eq!(strings(&delete), vec!["b"]);

        let expired = processor
            .process_arrow(batch(&[8, 8], &["x", "x"], &[INSERT, UPDATE_AFTER]), 200)
            .unwrap();
        assert_eq!(kinds(&expired), vec![INSERT, UPDATE_BEFORE, UPDATE_AFTER]);
        let after_ttl = processor
            .process_arrow(batch(&[8], &["x"], &[UPDATE_AFTER]), 211)
            .unwrap();
        assert_eq!(kinds(&after_ttl), vec![INSERT]);
        assert_eq!(processor.statistics()[2], 1);
        drop(first);
        drop(delete);
        drop(expired);
        drop(after_ttl);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn filter_deletes_the_previous_passing_value() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = ChangelogNormalizeProcessor::new(
            &plan_with_filter(),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "normalize filter test"),
        )
        .unwrap();
        let output = processor
            .process_arrow(
                filtered_batch(&[7, 7], &["visible", "hidden"], &[true, false]),
                100,
            )
            .unwrap();
        assert_eq!(kinds(&output), vec![INSERT, DELETE]);
        assert_eq!(strings(&output), vec!["visible", "visible"]);
    }

    #[test]
    fn canonical_state_restores_after_rescaling() {
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut source = processor(broker.clone(), 0, 127);
        source
            .process_arrow(batch(&[7, 8], &["a", "b"], &[INSERT, INSERT]), 100)
            .unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut low = processor(broker.clone(), 0, 63);
        let mut high = processor(broker, 64, 127);
        for (group, snapshot) in snapshots.iter().enumerate() {
            if group < 64 {
                low.restore_key_group(group as u32, snapshot).unwrap();
            } else {
                high.restore_key_group(group as u32, snapshot).unwrap();
            }
        }
        let mut output = Vec::new();
        for (key, value) in [(7, "c"), (8, "d")] {
            let update = batch(&[key], &[value], &[UPDATE_AFTER]);
            let encoded_key = source.state_key(&update, 0).unwrap();
            let key_group = assign_key_group(&encoded_key, 128);
            let result = if key_group < 64 {
                low.process_arrow(update, 101).unwrap()
            } else {
                high.process_arrow(update, 101).unwrap()
            };
            output.extend(kinds(&result));
        }
        assert_eq!(output, vec![UPDATE_AFTER, UPDATE_AFTER]);
    }

    #[test]
    fn canonical_state_moves_from_memory_to_rocksdb() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let broker = Arc::new(TestBroker::new(1 << 30));
        let mut memory = ChangelogNormalizeProcessor::new(
            &plan(true, 0),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "normalize memory source"),
        )
        .unwrap();
        memory
            .process_arrow(batch(&[7, 8], &["a", "b"], &[INSERT, INSERT]), 100)
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = ChangelogNormalizeProcessor::new_rocksdb(
            &plan(true, 0),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(broker, "normalize RocksDB scratch"),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(key_group as u32, snapshot).unwrap();
            assert_eq!(
                rocks.snapshot_key_group(key_group as u32).unwrap(),
                *snapshot
            );
        }
        let output = rocks
            .process_arrow(batch(&[7, 8], &["c", "d"], &[UPDATE_AFTER, DELETE]), 101)
            .unwrap();
        assert_eq!(kinds(&output), vec![UPDATE_BEFORE, UPDATE_AFTER, DELETE]);
        assert_eq!(strings(&output), vec!["a", "c", "b"]);
    }

    fn processor(broker: Arc<TestBroker>, first: u32, last: u32) -> ChangelogNormalizeProcessor {
        ChangelogNormalizeProcessor::new(
            &plan(false, 0),
            128,
            first,
            last,
            HostMemoryReservation::new(broker, "normalize rescale"),
        )
        .unwrap()
    }

    fn plan(generate_update_before: bool, ttl: u64) -> Vec<u8> {
        native_plan(proto::ChangelogNormalize {
            input: Some(Box::new(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            })),
            key_indices: vec![0],
            generate_update_before,
            input_schema: Some(schema()),
            state_ttl_millis: ttl,
            has_filter: false,
        })
    }

    fn plan_with_filter() -> Vec<u8> {
        native_plan(proto::ChangelogNormalize {
            input: Some(Box::new(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            })),
            key_indices: vec![0],
            generate_update_before: true,
            input_schema: Some(schema()),
            state_ttl_millis: 0,
            has_filter: true,
        })
    }

    fn native_plan(normalize: proto::ChangelogNormalize) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::ChangelogNormalize(Box::new(
                    normalize,
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                field(
                    "key",
                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                ),
                field(
                    "value",
                    proto::logical_type::Type::Varchar(proto::EmptyType::default()),
                ),
            ],
        }
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

    fn batch(keys: &[i64], values: &[&str], row_kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_input_row_kind",
                Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn filtered_batch(keys: &[i64], values: &[&str], filter: &[bool]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_input_row_kind",
                Arc::new(Int8Array::from(vec![INSERT; keys.len()])) as ArrayRef,
            ),
            (
                "__streamfusion_filter_result",
                Arc::new(BooleanArray::from(filter.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn kinds(batch: &RecordBatch) -> Vec<i8> {
        batch
            .column(batch.num_columns() - 2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn strings(batch: &RecordBatch) -> Vec<&str> {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect()
    }
}
