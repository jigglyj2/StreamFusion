// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, BinaryArray, BooleanArray, Int8Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

use super::calc::create_expression;

const INSERT: i8 = 0;
const STATE_MAGIC: &[u8; 4] = b"SFMR";
const STATE_VERSION: u8 = 1;

/// Strict fixed-sequence MATCH_RECOGNIZE with backend-neutral opaque Arrow-row state.
pub(crate) struct MatchRecognizeProcessor {
    plan: proto::MatchRecognize,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    visible_schema: SchemaRef,
    output_schema: SchemaRef,
    row_converter: RowConverter,
    conditions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    input_kind_index: Option<usize>,
    scratch_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
    completed_matches: u64,
}

#[derive(Default)]
struct PartitionState {
    candidates: Vec<Candidate>,
}

struct Candidate {
    rows: Vec<Vec<u8>>,
}

struct StagedPartition {
    key: StateKey,
    value: PartitionState,
    touched: bool,
}

impl MatchRecognizeProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = reservation.sibling("native match recognize batch scratch and output");
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
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("match recognize plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::MatchRecognize(plan)) => plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "match recognize handle requires a MatchRecognize root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        let visible_schema = arrow_schema(plan.input_schema.as_ref().expect("validated schema"))?;
        let visible_output_schema = arrow_schema(
            plan.output_schema
                .as_ref()
                .expect("validated output schema"),
        )?;
        let row_converter = row_converter(&visible_schema)?;
        let conditions = plan
            .variables
            .iter()
            .map(|variable| {
                variable
                    .condition
                    .as_ref()
                    .map(|condition| create_expression(condition, visible_schema.as_ref()))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut output_fields = visible_output_schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        Ok(Self {
            plan,
            max_parallelism,
            state,
            visible_schema,
            output_schema: Arc::new(Schema::new(output_fields)),
            row_converter,
            conditions,
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            input_kind_index: None,
            scratch_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
            completed_matches: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
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
            Ok(encoded) => self.process_accounted(&batch, &encoded),
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

    fn process_accounted(&mut self, batch: &RecordBatch, encoded: &Rows) -> Result<RecordBatch> {
        let kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("match recognize RowKinds are not Int8".to_string())
            })?;
        if let Some((row, kind)) = (0..batch.num_rows())
            .map(|row| (row, kinds.value(row)))
            .find(|(_, kind)| *kind != INSERT)
        {
            return Err(DataFusionError::Execution(format!(
                "match recognize requires an insert-only input, row {row} has Flink RowKind {kind}"
            )));
        }
        let condition_arrays = self
            .conditions
            .iter()
            .map(|condition| {
                condition
                    .as_ref()
                    .map(|condition| {
                        let array = condition.evaluate(batch)?.into_array(batch.num_rows())?;
                        array
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .cloned()
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "match recognize condition did not produce Boolean".to_string(),
                                )
                            })
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows().max(1),
            RandomState::new(),
        );
        let mut row_state_indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.partition_key(batch, row)?;
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
            .map(|key| key.expect("match recognize state index is populated"))
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
                Ok(StagedPartition {
                    key,
                    value: bytes
                        .map(|bytes| decode_state(bytes.as_ref()))
                        .transpose()?
                        .unwrap_or_default(),
                    touched: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let variable_count = self.plan.variables.len();
        let mut completed = Vec::<Vec<Vec<u8>>>::new();
        for row in 0..batch.num_rows() {
            let partition = &mut staged[row_state_indices[row]];
            let current = encoded.row(row).data();
            let mut next = Vec::with_capacity(partition.value.candidates.len() + 1);
            let mut matched = false;
            for mut candidate in partition.value.candidates.drain(..) {
                let stage = candidate.rows.len();
                if condition_matches(&condition_arrays[stage], row) {
                    candidate.rows.push(current.to_vec());
                    if candidate.rows.len() == variable_count {
                        completed.push(candidate.rows);
                        matched = true;
                    } else {
                        next.push(candidate);
                    }
                }
            }
            if !(matched && self.plan.skip_past_last_row)
                && condition_matches(&condition_arrays[0], row)
            {
                if variable_count == 1 {
                    completed.push(vec![current.to_vec()]);
                    matched = true;
                } else {
                    next.push(Candidate {
                        rows: vec![current.to_vec()],
                    });
                }
            }
            if matched && self.plan.skip_past_last_row {
                next.clear();
            }
            partition.value.candidates = next;
            partition.touched = true;
        }
        let mutations = staged
            .into_iter()
            .filter(|partition| partition.touched)
            .map(|partition| StateMutation {
                key: partition.key,
                value: (!partition.value.candidates.is_empty())
                    .then(|| encode_state(&partition.value)),
            })
            .collect::<Vec<_>>();
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        self.completed_matches = self
            .completed_matches
            .saturating_add(completed.len() as u64);
        self.output_batch(completed)
    }

    pub(crate) fn statistics(&self) -> [u64; 3] {
        [
            self.state_read_batches,
            self.state_write_batches,
            self.completed_matches,
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

    fn partition_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        match self.preencoded_key_index {
            Some(index) => Ok(batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "match recognize preencoded keys are not Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec()),
            None => Ok(encode_binary_row(batch, row, &self.key_fields)?),
        }
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "match recognize input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = metadata_index(&schema, "__streamfusion_key");
        self.input_kind_index = metadata_index(&schema, "__streamfusion_input_row_kind");
        if self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "match recognize requires RowKind metadata".to_string(),
            ));
        }
        let visible_count = [
            self.preencoded_key_index,
            self.input_kind_index,
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
                "match recognize Arrow schema does not match its protobuf schema".to_string(),
            ));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .partition_key_indices
                .iter()
                .map(|&index| {
                    let field = schema.fields().get(index as usize).ok_or_else(|| {
                        arrow::error::ArrowError::SchemaError(format!(
                            "match recognize partition key {index} is outside the visible row"
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

    fn output_batch(&self, matches: Vec<Vec<Vec<u8>>>) -> Result<RecordBatch> {
        if matches.is_empty() {
            return self.empty_output();
        }
        let parser = self.row_converter.parser();
        let variable_columns = (0..self.plan.variables.len())
            .map(|variable| {
                self.row_converter.convert_rows(
                    matches
                        .iter()
                        .map(|matched| parser.parse(&matched[variable])),
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut columns = Vec::with_capacity(self.output_schema.fields().len());
        for &key in &self.plan.partition_key_indices {
            columns.push(variable_columns[0][key as usize].clone());
        }
        for measure in &self.plan.measures {
            columns.push(
                variable_columns[measure.variable_index as usize][measure.field_index as usize]
                    .clone(),
            );
        }
        columns.push(Arc::new(Int8Array::from(vec![INSERT; matches.len()])));
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

fn condition_matches(condition: &Option<BooleanArray>, row: usize) -> bool {
    condition
        .as_ref()
        .map(|condition| !condition.is_null(row) && condition.value(row))
        .unwrap_or(true)
}

fn validate_plan(plan: &proto::MatchRecognize, max_parallelism: u32) -> Result<()> {
    let input = plan.input_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("match recognize input schema is missing".to_string())
    })?;
    let output = plan.output_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("match recognize output schema is missing".to_string())
    })?;
    if max_parallelism == 0 || plan.variables.is_empty() {
        return Err(DataFusionError::Plan(
            "match recognize max parallelism and variables must be non-empty".to_string(),
        ));
    }
    if plan.partition_key_indices.len() + plan.measures.len() != output.fields.len() {
        return Err(DataFusionError::Plan(
            "match recognize output must contain partition columns followed by measures"
                .to_string(),
        ));
    }
    for &key in &plan.partition_key_indices {
        if key as usize >= input.fields.len() {
            return Err(DataFusionError::Plan(format!(
                "match recognize partition key {key} is outside the input schema"
            )));
        }
    }
    for measure in &plan.measures {
        if measure.variable_index as usize >= plan.variables.len()
            || measure.field_index as usize >= input.fields.len()
        {
            return Err(DataFusionError::Plan(
                "match recognize measure references an unknown variable or field".to_string(),
            ));
        }
    }
    Ok(())
}

fn row_converter(schema: &Schema) -> Result<RowConverter> {
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

fn metadata_index(schema: &Schema, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn encode_state(state: &PartitionState) -> Vec<u8> {
    let capacity = 9usize.saturating_add(
        state
            .candidates
            .iter()
            .flat_map(|candidate| candidate.rows.iter())
            .map(|row| 4usize.saturating_add(row.len()))
            .sum::<usize>(),
    );
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.push(STATE_VERSION);
    bytes.extend_from_slice(&(state.candidates.len() as u32).to_le_bytes());
    for candidate in &state.candidates {
        bytes.extend_from_slice(&(candidate.rows.len() as u32).to_le_bytes());
        for row in &candidate.rows {
            bytes.extend_from_slice(&(row.len() as u32).to_le_bytes());
            bytes.extend_from_slice(row);
        }
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<PartitionState> {
    if bytes.len() < 9 || &bytes[..4] != STATE_MAGIC || bytes[4] != STATE_VERSION {
        return Err(DataFusionError::Execution(
            "match recognize state has an incompatible header".to_string(),
        ));
    }
    let mut offset = 5;
    let candidates = read_u32(bytes, &mut offset)? as usize;
    let mut decoded = Vec::with_capacity(candidates);
    for _ in 0..candidates {
        let row_count = read_u32(bytes, &mut offset)? as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let length = read_u32(bytes, &mut offset)? as usize;
            let end = offset.checked_add(length).ok_or_else(|| {
                DataFusionError::Execution("match recognize state length overflow".to_string())
            })?;
            let row = bytes.get(offset..end).ok_or_else(|| {
                DataFusionError::Execution("truncated match recognize row state".to_string())
            })?;
            rows.push(row.to_vec());
            offset = end;
        }
        decoded.push(Candidate { rows });
    }
    if offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "match recognize state has trailing bytes".to_string(),
        ));
    }
    Ok(PartitionState {
        candidates: decoded,
    })
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.checked_add(4).ok_or_else(|| {
        DataFusionError::Execution("match recognize state offset overflow".to_string())
    })?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated match recognize state".to_string()))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        value.try_into().expect("four-byte slice"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use prost::Message;

    #[test]
    fn fixed_sequence_matches_and_skip_strategy_is_exact() {
        let mut next = processor(false, 0, 127);
        let overlapping = next
            .process_arrow(batch(&[7, 7, 7, 7], &[1, 2, 3, 4], &["x", "x", "x", "x"]))
            .unwrap();
        assert_eq!(ids(&overlapping, 1), vec![1, 2]);
        assert_eq!(ids(&overlapping, 2), vec![2, 3]);
        assert_eq!(ids(&overlapping, 3), vec![3, 4]);

        let mut past = processor(true, 0, 127);
        let non_overlapping = past
            .process_arrow(batch(&[7, 7, 7], &[1, 2, 3], &["x", "x", "x"]))
            .unwrap();
        assert_eq!(ids(&non_overlapping, 1), vec![1]);
        assert_eq!(ids(&non_overlapping, 2), vec![2]);
    }

    #[test]
    fn canonical_partial_matches_restore_after_one_to_two_and_back_to_one() {
        let mut source = processor(true, 0, 127);
        source
            .process_arrow(batch(
                &[7, 8, 7, 8],
                &[10, 20, 11, 21],
                &["a", "a", "b", "b"],
            ))
            .unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut low = processor(true, 0, 63);
        let mut high = processor(true, 64, 127);
        for (group, snapshot) in snapshots.iter().enumerate() {
            if group < 64 {
                low.restore_key_group(group as u32, snapshot).unwrap();
            } else {
                high.restore_key_group(group as u32, snapshot).unwrap();
            }
        }
        let mut outputs = Vec::new();
        for (key, id) in [(7, 12), (8, 22)] {
            let final_row = batch(&[key], &[id], &["c"]);
            let partition = source.partition_key(&final_row, 0).unwrap();
            let output = if assign_key_group(&partition, 128) < 64 {
                low.process_arrow(final_row).unwrap()
            } else {
                high.process_arrow(final_row).unwrap()
            };
            outputs.extend(ids(&output, 3));
        }
        outputs.sort_unstable();
        assert_eq!(outputs, vec![12, 22]);

        for (key, first_id) in [(7, 13), (8, 23)] {
            let partial = batch(&[key, key], &[first_id, first_id + 1], &["a", "b"]);
            let partition = source.partition_key(&partial, 0).unwrap();
            if assign_key_group(&partition, 128) < 64 {
                low.process_arrow(partial).unwrap();
            } else {
                high.process_arrow(partial).unwrap();
            }
        }
        let mut merged = processor(true, 0, 127);
        for group in 0..128 {
            let snapshot = if group < 64 {
                low.snapshot_key_group(group).unwrap()
            } else {
                high.snapshot_key_group(group).unwrap()
            };
            merged.restore_key_group(group, &snapshot).unwrap();
        }
        let resumed = merged
            .process_arrow(batch(&[7, 8], &[15, 25], &["c", "c"]))
            .unwrap();
        let mut final_ids = ids(&resumed, 3);
        final_ids.sort_unstable();
        assert_eq!(final_ids, vec![15, 25]);
    }

    #[test]
    fn state_codec_rejects_corruption_and_round_trips_candidates() {
        let value = PartitionState {
            candidates: vec![Candidate {
                rows: vec![vec![1, 2], vec![3, 4, 5]],
            }],
        };
        let encoded = encode_state(&value);
        let decoded = decode_state(&encoded).unwrap();
        assert_eq!(decoded.candidates.len(), 1);
        assert_eq!(decoded.candidates[0].rows, value.candidates[0].rows);
        assert!(decode_state(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn accounts_partial_state_batch_scratch_and_exported_output() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = MatchRecognizeProcessor::new(
            &plan(true),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "match recognize accounting test"),
        )
        .unwrap();

        let empty = processor
            .process_arrow(batch(&[7, 7], &[1, 2], &["a", "b"]))
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert!(
            broker.reserved() > 0,
            "partial match state must be reserved"
        );
        drop(empty);

        let output = processor.process_arrow(batch(&[7], &[3], &["c"])).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert!(
            broker.reserved() > 0,
            "exported Arrow output must stay reserved"
        );
        drop(output);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    fn processor(skip_past: bool, first: u32, last: u32) -> MatchRecognizeProcessor {
        MatchRecognizeProcessor::new(
            &plan(skip_past),
            128,
            first,
            last,
            HostMemoryReservation::new(Arc::new(TestBroker::new(64 << 20)), "match recognize test"),
        )
        .unwrap()
    }

    fn plan(skip_past: bool) -> Vec<u8> {
        let schema = input_schema();
        let output = proto::Schema {
            fields: vec![
                schema.fields[0].clone(),
                schema.fields[1].clone(),
                schema.fields[1].clone(),
                schema.fields[1].clone(),
            ],
        };
        let match_plan = proto::MatchRecognize {
            partition_key_indices: vec![0],
            variables: ["A", "B", "C"]
                .into_iter()
                .map(|name| proto::MatchPatternVariable {
                    name: name.to_string(),
                    condition: None,
                })
                .collect(),
            measures: (0..3)
                .map(|variable| proto::MatchMeasure {
                    variable_index: variable,
                    field_index: 1,
                })
                .collect(),
            input_schema: Some(schema),
            output_schema: Some(output),
            skip_past_last_row: skip_past,
        };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::MatchRecognize(match_plan)),
            }),
        }
        .encode_to_vec()
    }

    fn input_schema() -> proto::Schema {
        proto::Schema {
            fields: vec![
                field(
                    "key",
                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                ),
                field(
                    "id",
                    proto::logical_type::Type::Bigint(proto::EmptyType::default()),
                ),
                field(
                    "label",
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

    fn batch(keys: &[i64], ids: &[i64], labels: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
            (
                "label",
                Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
            ),
            (
                "__streamfusion_input_row_kind",
                Arc::new(Int8Array::from(vec![INSERT; keys.len()])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn ids(batch: &RecordBatch, column: usize) -> Vec<i64> {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }
}
