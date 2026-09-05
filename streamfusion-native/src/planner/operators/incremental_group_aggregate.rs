// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int8Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;
use prost::Message;

use super::group_aggregate::{
    decode_state, encode_state, lower_call, sort_flink_hashmap_keys, AccumulatorState, Call,
};
use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::proto;
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};

const INSERT: i8 = 0;

/// Stateful middle stage of Flink's local/incremental/global mini-batch aggregate chain.
///
/// Local accumulator deltas are merged into canonical per-split-key state. At each bundle
/// boundary this stage emits one opaque net accumulator delta per final key, so duplicate
/// DISTINCT values and retractable extrema remain correct across bundles without exposing
/// accumulator contents to Java.
pub(crate) struct IncrementalGroupAggregateProcessor {
    plan: proto::IncrementalGroupAggregate,
    partial_calls: Vec<Call>,
    final_calls: Vec<Call>,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    final_converter: RowConverter,
    key_fields: Vec<(usize, KeyField)>,
    final_key_fields: Vec<(usize, KeyField)>,
    pending: HashMap<StateKey, PendingPartial, RandomState>,
    pending_order: Vec<StateKey>,
    pending_elements: usize,
    scratch_reservation: HostMemoryReservation,
    bundle_reservation: HostMemoryReservation,
    state_read_batches: u64,
    state_write_batches: u64,
}

struct PendingPartial {
    final_key: Vec<u8>,
    grouping_row: Vec<u8>,
    original: Option<AccumulatorState>,
    state_loaded: bool,
    current: Option<AccumulatorState>,
    bundle: AccumulatorState,
}

struct PendingFinal {
    grouping_row: Vec<u8>,
    accumulator: AccumulatorState,
}

impl IncrementalGroupAggregateProcessor {
    pub(crate) fn new(
        plan_bytes: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let scratch = state_reservation.sibling("native incremental aggregate scratch");
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            state_reservation,
        )?);
        Self::with_state(plan_bytes, max_parallelism, state, scratch)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_rocksdb(
        plan_bytes: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(plan_bytes, max_parallelism, state, scratch_reservation)
    }

    fn with_state(
        plan_bytes: &[u8],
        max_parallelism: u32,
        state: Box<dyn KeyedState>,
        scratch_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(DataFusionError::Plan(
                "incremental aggregate max parallelism must be positive".to_string(),
            ));
        }
        let native = proto::NativePlan::decode(plan_bytes)
            .map_err(|error| DataFusionError::Plan(format!("invalid native plan: {error}")))?;
        if native.protocol_version != crate::PLAN_PROTOCOL_VERSION {
            return Err(DataFusionError::Plan(format!(
                "unsupported plan protocol version {}",
                native.protocol_version
            )));
        }
        let plan = match native.root.and_then(|operator| operator.operator) {
            Some(proto::operator::Operator::IncrementalGroupAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "native plan root is not an incremental group aggregate".to_string(),
                ));
            }
        };
        if plan.mini_batch_size == 0 || plan.mini_batch_size > usize::MAX as u64 {
            return Err(DataFusionError::Plan(
                "incremental aggregate requires a positive mini-batch size".to_string(),
            ));
        }
        let input_schema =
            crate::planner::arrow_schema(plan.input_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("incremental aggregate requires an input schema".to_string())
            })?)?;
        let visible_output =
            crate::planner::arrow_schema(plan.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("incremental aggregate requires an output schema".to_string())
            })?)?;
        let partial_count = plan.partial_grouping_count as usize;
        if input_schema.fields().len() != partial_count + 1
            || input_schema
                .fields()
                .last()
                .is_none_or(|field| field.data_type() != &DataType::Binary)
        {
            return Err(DataFusionError::Plan(
                "incremental input must contain partial grouping fields and one BINARY accumulator"
                    .to_string(),
            ));
        }
        if visible_output.fields().len() != plan.final_grouping_indices.len() + 1
            || visible_output
                .fields()
                .last()
                .is_none_or(|field| field.data_type() != &DataType::Binary)
        {
            return Err(DataFusionError::Plan(
                "incremental output must contain final grouping fields and one BINARY accumulator"
                    .to_string(),
            ));
        }
        if plan.final_aggregate_calls.len() != plan.final_call_value_indices.len() {
            return Err(DataFusionError::Plan(
                "incremental final calls and partial-value mappings must be equally sized"
                    .to_string(),
            ));
        }
        let final_fields = plan
            .final_grouping_indices
            .iter()
            .map(|&index| {
                input_schema
                    .fields()
                    .get(index as usize)
                    .filter(|_| (index as usize) < partial_count)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "incremental final grouping index {index} is outside the partial key"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if !RowConverter::supports_fields(&final_fields) {
            return Err(DataFusionError::Plan(
                "incremental grouping type is not supported by Arrow row encoding".to_string(),
            ));
        }
        let key_fields = lower_key_fields(&input_schema, 0..partial_count);
        let final_key_fields = lower_key_fields(
            &input_schema,
            plan.final_grouping_indices
                .iter()
                .map(|index| *index as usize),
        );
        let partial_calls = plan
            .partial_aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        let final_calls = plan
            .final_aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        for &index in &plan.final_call_value_indices {
            if index != u32::MAX && index as usize >= partial_calls.len() {
                return Err(DataFusionError::Plan(format!(
                    "incremental partial-value index {index} is outside its aggregate results"
                )));
            }
        }
        let mut output_fields = visible_output.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        let bundle_reservation = scratch_reservation.sibling("native incremental aggregate bundle");
        Ok(Self {
            plan,
            partial_calls,
            final_calls,
            max_parallelism,
            state,
            input_schema,
            output_schema: Arc::new(Schema::new(output_fields)),
            final_converter: RowConverter::new(final_fields)?,
            key_fields,
            final_key_fields,
            pending: HashMap::with_hasher(RandomState::new()),
            pending_order: Vec::new(),
            pending_elements: 0,
            scratch_reservation,
            bundle_reservation,
            state_read_batches: 0,
            state_write_batches: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.validate_batch(&batch)?;
        let final_rows = self.final_rows(&batch)?;
        let mut output_keys = Vec::new();
        let mut output_accumulators = Vec::new();
        let trigger = self.plan.mini_batch_size as usize;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let length = (trigger - self.pending_elements).min(batch.num_rows() - offset);
            self.stage_range(&batch, &final_rows, offset, length)?;
            self.pending_elements += length;
            offset += length;
            if self.pending_elements == trigger {
                self.finish_pending(&mut output_keys, &mut output_accumulators)?;
            }
        }
        self.resize_bundle_reservation()?;
        self.output_batch(output_keys, output_accumulators)
    }

    pub(crate) fn finish_bundle(&mut self) -> Result<RecordBatch> {
        let mut keys = Vec::new();
        let mut accumulators = Vec::new();
        self.finish_pending(&mut keys, &mut accumulators)?;
        self.bundle_reservation.resize(0)?;
        self.output_batch(keys, accumulators)
    }

    pub(crate) fn pending_element_count(&self) -> usize {
        self.pending_elements
    }

    pub(crate) fn pending_key_count(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn statistics(&self) -> [u64; 2] {
        [self.state_read_batches, self.state_write_batches]
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

    fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() < self.input_schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "incremental aggregate expected at least {} columns but received {}",
                self.input_schema.fields().len(),
                batch.num_columns()
            )));
        }
        for (index, expected) in self.input_schema.fields().iter().enumerate() {
            if batch.schema().field(index).data_type() != expected.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "incremental aggregate input field {index} has type {} instead of {}",
                    batch.schema().field(index).data_type(),
                    expected.data_type()
                )));
            }
        }
        let has_preencoded = batch
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == "__streamfusion_key");
        if !has_preencoded && self.key_fields.len() != self.plan.partial_grouping_count as usize {
            return Err(DataFusionError::Plan(
                "incremental aggregate requires Flink BinaryRow key metadata for this key type"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn stage_range(
        &mut self,
        batch: &RecordBatch,
        final_rows: &Rows,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let accumulators = batch
            .column(self.plan.partial_grouping_count as usize)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("incremental accumulator type was validated");
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            length,
            RandomState::new(),
        );
        let mut first_rows = Vec::new();
        let mut row_indices = Vec::with_capacity(length);
        for row in offset..offset + length {
            let key = self.state_key(batch, row)?;
            let next = unique.len();
            let index = match unique.entry(key) {
                hashbrown::hash_map::Entry::Occupied(entry) => *entry.get(),
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(next);
                    first_rows.push(row);
                    next
                }
            };
            row_indices.push(index);
        }
        let mut ordered_keys = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
        for (key, index) in unique.drain() {
            ordered_keys[index] = Some(key);
        }
        let keys = ordered_keys
            .into_iter()
            .map(|key| key.expect("every incremental key index is populated"))
            .collect::<Vec<_>>();
        let missing = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| (!self.pending.contains_key(key)).then_some(index))
            .collect::<Vec<_>>();
        for &index in &missing {
            self.pending_order.push(keys[index].clone());
            self.pending.insert(
                keys[index].clone(),
                PendingPartial {
                    final_key: self.final_key(batch, first_rows[index])?,
                    grouping_row: final_rows.row(first_rows[index]).as_ref().to_vec(),
                    current: None,
                    original: None,
                    state_loaded: false,
                    bundle: AccumulatorState::new(&self.partial_calls),
                },
            );
        }
        for (local_row, &key_index) in row_indices.iter().enumerate() {
            let row = offset + local_row;
            if accumulators.is_null(row) {
                return Err(DataFusionError::Execution(
                    "incremental local accumulator cannot be null".to_string(),
                ));
            }
            let delta = decode_state(accumulators.value(row), &self.partial_calls)?;
            let pending = self
                .pending
                .get_mut(&keys[key_index])
                .expect("incremental key was staged");
            if pending.current.is_some()
                || delta.has_incremental_persistent_state(&self.partial_calls)
            {
                pending
                    .current
                    .get_or_insert_with(|| AccumulatorState::new(&self.partial_calls))
                    .merge(&self.partial_calls, &delta)?;
            }
            pending.bundle.merge(&self.partial_calls, &delta)?;
        }
        // Ordinary COUNT/SUM/AVG and append-only extrema branches are bundle-local. Delay the
        // state read until after merging the Arrow range so only keys with a net DISTINCT or
        // retractable-extremum change reach MemoryKeyedState or RocksDB. A key whose persistent
        // changes cancel remains deliberately unknown: if a later range changes it again, this
        // same check will load its persistent state then.
        let stateful_unknown = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| {
                let pending = self
                    .pending
                    .get(key)
                    .expect("incremental key was staged before state lookup");
                (!pending.state_loaded
                    && pending.current.as_ref().is_some_and(|current| {
                        current.has_incremental_persistent_state(&self.partial_calls)
                    }))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        if !stateful_unknown.is_empty() {
            let refs = stateful_unknown
                .iter()
                .map(|&index| StateKeyRef {
                    key_group: keys[index].key_group,
                    key: &keys[index].key,
                })
                .collect::<Vec<_>>();
            self.state_read_batches = self.state_read_batches.saturating_add(1);
            let existing = self.state.get_batch(&refs)?;
            for (&index, value) in stateful_unknown.iter().zip(existing) {
                let pending = self
                    .pending
                    .get_mut(&keys[index])
                    .expect("incremental key remains staged during state lookup");
                pending.state_loaded = true;
                if let Some(value) = value {
                    let original = decode_state(&value, &self.partial_calls)?
                        .incremental_persistent_only(&self.partial_calls);
                    let mut current = original.clone();
                    current.merge(
                        &self.partial_calls,
                        pending
                            .current
                            .as_ref()
                            .expect("persistent change initialized current accumulator"),
                    )?;
                    pending.original = Some(original);
                    pending.current = Some(current);
                }
            }
        }
        Ok(())
    }

    fn finish_pending(
        &mut self,
        output_keys: &mut Vec<Vec<u8>>,
        output_accumulators: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let mut partial_order = std::mem::take(&mut self.pending_order);
        sort_flink_hashmap_keys(&mut partial_order, |key| &key.key);
        let mut finals =
            HashMap::<Vec<u8>, PendingFinal, RandomState>::with_hasher(RandomState::new());
        let mut final_order = Vec::new();
        let mut mutations = Vec::with_capacity(partial_order.len());
        for state_key in partial_order {
            let partial = self
                .pending
                .remove(&state_key)
                .expect("incremental order and map remain synchronized");
            let entry = match finals.entry(partial.final_key) {
                hashbrown::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    final_order.push(entry.key().clone());
                    entry.insert(PendingFinal {
                        grouping_row: partial.grouping_row,
                        accumulator: AccumulatorState::new(&self.final_calls),
                    })
                }
            };
            if let Some(original) = partial.original.as_ref() {
                let delta = AccumulatorState::from_mapped_partial(
                    &self.final_calls,
                    &self.partial_calls,
                    original,
                    &partial.bundle,
                    &self.plan.final_call_value_indices,
                    true,
                    0,
                    false,
                )?;
                entry.accumulator.merge(&self.final_calls, &delta)?;
            }
            let current = partial.current.as_ref().unwrap_or(&partial.bundle);
            let delta = AccumulatorState::from_mapped_partial(
                &self.final_calls,
                &self.partial_calls,
                current,
                &partial.bundle,
                &self.plan.final_call_value_indices,
                false,
                partial.bundle.row_count,
                true,
            )?;
            entry.accumulator.merge(&self.final_calls, &delta)?;
            let persistent = partial
                .current
                .as_ref()
                .map(|current| current.incremental_persistent_only(&self.partial_calls));
            let has_persistent = persistent.as_ref().is_some_and(|persistent| {
                persistent.has_incremental_persistent_state(&self.partial_calls)
            });
            if partial.original.is_some() || has_persistent {
                mutations.push(StateMutation {
                    key: state_key,
                    value: persistent
                        .as_ref()
                        .filter(|_| has_persistent)
                        .map(encode_state),
                });
            }
        }
        if !mutations.is_empty() {
            self.state.write_batch(mutations)?;
            self.state_write_batches = self.state_write_batches.saturating_add(1);
        }
        sort_flink_hashmap_keys(&mut final_order, Vec::as_slice);
        for key in final_order {
            let final_group = finals
                .remove(&key)
                .expect("incremental final order and map remain synchronized");
            output_keys.push(final_group.grouping_row);
            output_accumulators.push(encode_state(&final_group.accumulator));
        }
        self.pending_elements = 0;
        Ok(())
    }

    fn state_key(&self, batch: &RecordBatch, row: usize) -> Result<StateKey> {
        let key = if let Some(index) = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key")
        {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "incremental preencoded key metadata must be Binary".to_string(),
                    )
                })?
                .value(row)
                .to_vec()
        } else {
            encode_binary_row(batch, row, &self.key_fields)?
        };
        Ok(StateKey {
            key_group: assign_key_group(&key, self.max_parallelism),
            key,
        })
    }

    fn final_rows(&self, batch: &RecordBatch) -> Result<Rows> {
        let columns = self
            .plan
            .final_grouping_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index as usize)))
            .collect::<Vec<_>>();
        Ok(self.final_converter.convert_columns(&columns)?)
    }

    fn final_key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        if self.final_key_fields.len() != self.plan.final_grouping_indices.len() {
            return Err(DataFusionError::Plan(
                "incremental aggregate requires native Flink key encoding for its final grouping type"
                    .to_string(),
            ));
        }
        Ok(encode_binary_row(batch, row, &self.final_key_fields)?)
    }

    fn resize_bundle_reservation(&mut self) -> Result<()> {
        let bytes = self
            .pending
            .capacity()
            .saturating_mul(std::mem::size_of::<(StateKey, PendingPartial)>() + 16)
            .saturating_add(
                self.pending_order
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StateKey>()),
            )
            .saturating_add(self.pending.iter().fold(0usize, |bytes, (key, value)| {
                bytes
                    .saturating_add(key.key.capacity().saturating_mul(2))
                    .saturating_add(value.final_key.capacity())
                    .saturating_add(value.grouping_row.capacity())
                    .saturating_add(
                        value
                            .current
                            .as_ref()
                            .map_or(0, AccumulatorState::estimated_dynamic_bytes),
                    )
                    .saturating_add(value.bundle.estimated_dynamic_bytes())
                    .saturating_add(
                        value
                            .original
                            .as_ref()
                            .map_or(0, AccumulatorState::estimated_dynamic_bytes),
                    )
            }));
        self.bundle_reservation.resize(bytes)
    }

    fn output_batch(
        &mut self,
        keys: Vec<Vec<u8>>,
        accumulators: Vec<Vec<u8>>,
    ) -> Result<RecordBatch> {
        let mut columns = if self.plan.final_grouping_indices.is_empty() {
            Vec::new()
        } else {
            let parser = self.final_converter.parser();
            self.final_converter
                .convert_rows(keys.iter().map(|key| parser.parse(key)))?
        };
        columns.push(Arc::new(BinaryArray::from_iter_values(
            accumulators.iter().map(Vec::as_slice),
        )) as ArrayRef);
        columns.push(Arc::new(Int8Array::from(vec![INSERT; keys.len()])) as ArrayRef);
        let output = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)?;
        let bytes = output.get_array_memory_size();
        self.scratch_reservation.resize(bytes)?;
        self.scratch_reservation.transfer_to_arrow(bytes)?;
        self.scratch_reservation.resize(0)?;
        Ok(output)
    }
}

fn lower_key_fields(
    schema: &SchemaRef,
    indices: impl IntoIterator<Item = usize>,
) -> Vec<(usize, KeyField)> {
    indices
        .into_iter()
        .map(|index| {
            schema
                .fields()
                .get(index)
                .ok_or_else(|| {
                    arrow::error::ArrowError::SchemaError(format!(
                        "incremental grouping index {index} is outside its input"
                    ))
                })
                .and_then(|field| {
                    KeyField::from_arrow_type(field.data_type()).map(|key_field| (index, key_field))
                })
        })
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use crate::planner::operators::group_aggregate::AggregateValue;
    use arrow::array::{Int64Array, Int8Array};

    fn bigint(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
        }
    }

    fn binary(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Binary(proto::EmptyType {})),
        }
    }

    fn schema(fields: &[(&str, proto::LogicalType)]) -> proto::Schema {
        proto::Schema {
            fields: fields
                .iter()
                .map(|(name, logical)| proto::Field {
                    name: (*name).to_string(),
                    r#type: Some(logical.clone()),
                })
                .collect(),
        }
    }

    fn calls() -> (proto::AggregateCall, proto::AggregateCall) {
        (
            proto::AggregateCall {
                function: proto::AggregateFunction::Count as i32,
                input_index: Some(1),
                input_type: Some(bigint(true)),
                output_type: Some(bigint(false)),
                retractable: true,
                filter_index: None,
                distinct: true,
                accumulator_type: None,
            },
            proto::AggregateCall {
                function: proto::AggregateFunction::Sum0 as i32,
                input_index: Some(2),
                input_type: Some(bigint(false)),
                output_type: Some(bigint(false)),
                retractable: true,
                filter_index: None,
                distinct: false,
                accumulator_type: None,
            },
        )
    }

    fn plan(size: u64) -> Vec<u8> {
        let (partial, final_call) = calls();
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::IncrementalGroupAggregate(
                    Box::new(proto::IncrementalGroupAggregate {
                        input: None,
                        partial_grouping_count: 2,
                        final_grouping_indices: vec![0],
                        partial_aggregate_calls: vec![partial],
                        final_aggregate_calls: vec![final_call],
                        final_call_value_indices: vec![0],
                        mini_batch_size: size,
                        input_schema: Some(schema(&[
                            ("final_key", bigint(false)),
                            ("split_key", bigint(false)),
                            ("accumulator", binary(false)),
                        ])),
                        output_schema: Some(schema(&[
                            ("final_key", bigint(false)),
                            ("accumulator", binary(false)),
                        ])),
                    }),
                )),
            }),
        }
        .encode_to_vec()
    }

    fn ordinary_plan(size: u64) -> Vec<u8> {
        let (mut partial, final_call) = calls();
        partial.distinct = false;
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::IncrementalGroupAggregate(
                    Box::new(proto::IncrementalGroupAggregate {
                        input: None,
                        partial_grouping_count: 2,
                        final_grouping_indices: vec![0],
                        partial_aggregate_calls: vec![partial],
                        final_aggregate_calls: vec![final_call],
                        final_call_value_indices: vec![0],
                        mini_batch_size: size,
                        input_schema: Some(schema(&[
                            ("final_key", bigint(false)),
                            ("split_key", bigint(false)),
                            ("accumulator", binary(false)),
                        ])),
                        output_schema: Some(schema(&[
                            ("final_key", bigint(false)),
                            ("accumulator", binary(false)),
                        ])),
                    }),
                )),
            }),
        }
        .encode_to_vec()
    }

    fn local_delta(value: i64, accumulate: bool) -> Vec<u8> {
        let call = lower_call(&calls().0).unwrap();
        let mut state = AccumulatorState::new(std::slice::from_ref(&call));
        state
            .apply_values(
                std::slice::from_ref(&call),
                &[Some(AggregateValue::Int(value as i128))],
                accumulate,
            )
            .unwrap();
        encode_state(&state)
    }

    fn ordinary_count_delta(value: i64) -> Vec<u8> {
        let mut call = lower_call(&calls().0).unwrap();
        call.distinct = false;
        encode_state(
            &AccumulatorState::from_partial_values(
                std::slice::from_ref(&call),
                &[Some(AggregateValue::Int(value as i128))],
                value,
                true,
            )
            .unwrap(),
        )
    }

    fn batch(rows: &[(i64, i64, Vec<u8>)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("final_key", DataType::Int64, false),
                Field::new("split_key", DataType::Int64, false),
                Field::new("accumulator", DataType::Binary, false),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(BinaryArray::from_iter_values(
                    rows.iter().map(|row| row.2.as_slice()),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn processor(size: u64, broker: Arc<TestBroker>) -> IncrementalGroupAggregateProcessor {
        IncrementalGroupAggregateProcessor::new(
            &plan(size),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "incremental aggregate test"),
        )
        .unwrap()
    }

    fn merge_output(global: &mut AccumulatorState, output: &RecordBatch) {
        let call = lower_call(&calls().1).unwrap();
        let encoded = output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..output.num_rows() {
            global
                .merge(
                    std::slice::from_ref(&call),
                    &decode_state(encoded.value(row), std::slice::from_ref(&call)).unwrap(),
                )
                .unwrap();
        }
    }

    #[test]
    fn suppresses_duplicate_distinct_values_across_bundles_and_retracts() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = processor(1, broker);
        let final_call = lower_call(&calls().1).unwrap();
        let mut global = AccumulatorState::new(std::slice::from_ref(&final_call));
        for (delta, expected) in [
            ((7, true), 1),
            ((7, true), 1),
            ((7, false), 1),
            ((7, false), 0),
        ] {
            let output = processor
                .process_arrow(batch(&[(3, 11, local_delta(delta.0, delta.1))]))
                .unwrap();
            assert_eq!(output.num_rows(), 1);
            merge_output(&mut global, &output);
            assert_eq!(
                global.values(std::slice::from_ref(&final_call)),
                vec![Some(AggregateValue::Int(expected))]
            );
        }
        assert_eq!(global.row_count, 0);
        assert_eq!(processor.statistics(), [4, 4]);
    }

    #[test]
    fn canonical_state_restores_after_key_group_rescaling_and_accounts_memory() {
        let source_broker = Arc::new(TestBroker::new(64 << 20));
        let mut source = processor(8, Arc::clone(&source_broker));
        source
            .process_arrow(batch(&[(9, 17, local_delta(42, true))]))
            .unwrap();
        assert!(source_broker.reserved() > 0);
        source.finish_bundle().unwrap();

        let key_batch = batch(&[(9, 17, local_delta(42, true))]);
        let key = encode_binary_row(
            &key_batch,
            0,
            &[(0, KeyField::BigInt), (1, KeyField::BigInt)],
        )
        .unwrap();
        let key_group = assign_key_group(&key, 128);
        let snapshot = source.snapshot_key_group(key_group).unwrap();
        let target_broker = Arc::new(TestBroker::new(64 << 20));
        let mut target = IncrementalGroupAggregateProcessor::new(
            &plan(1),
            128,
            key_group,
            key_group,
            HostMemoryReservation::new(target_broker.clone(), "restored incremental test"),
        )
        .unwrap();
        target.restore_key_group(key_group, &snapshot).unwrap();
        let duplicate = target
            .process_arrow(batch(&[(9, 17, local_delta(42, true))]))
            .unwrap();
        let encoded = duplicate
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let final_call = lower_call(&calls().1).unwrap();
        let delta = decode_state(encoded.value(0), std::slice::from_ref(&final_call)).unwrap();
        // The DISTINCT value contributes no new aggregate value, but the duplicate input still
        // contributes to the final group's lifecycle count.
        assert_eq!(delta.row_count, 1);
        assert_eq!(target.statistics(), [1, 1]);
        assert!(target_broker.reserved() > 0);
        assert_eq!(
            duplicate
                .column(2)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            INSERT
        );
    }

    #[test]
    fn canonical_state_moves_from_memory_to_rocksdb_with_batched_io() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let mut memory = processor(1, Arc::new(TestBroker::new(64 << 20)));
        memory
            .process_arrow(batch(&[(5, 23, local_delta(99, true))]))
            .unwrap();
        let snapshots = (0..128)
            .map(|key_group| memory.snapshot_key_group(key_group).unwrap())
            .collect::<Vec<_>>();

        let directory = tempfile::tempdir().unwrap();
        let mut rocks = IncrementalGroupAggregateProcessor::new_rocksdb(
            &plan(1),
            128,
            0,
            127,
            std::path::Path::new(&plugin_path),
            directory.path(),
            64 << 20,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(256 << 20)),
                "incremental RocksDB test",
            ),
        )
        .unwrap();
        for (key_group, snapshot) in snapshots.iter().enumerate() {
            rocks.restore_key_group(key_group as u32, snapshot).unwrap();
        }
        let duplicate = rocks
            .process_arrow(batch(&[(5, 23, local_delta(99, true))]))
            .unwrap();
        let final_call = lower_call(&calls().1).unwrap();
        let encoded = duplicate
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(
            decode_state(encoded.value(0), std::slice::from_ref(&final_call))
                .unwrap()
                .row_count,
            1
        );
        assert_eq!(rocks.statistics(), [1, 1]);
    }

    #[test]
    fn ordinary_split_branches_do_not_touch_persistent_state() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = IncrementalGroupAggregateProcessor::new(
            &ordinary_plan(1),
            128,
            0,
            127,
            HostMemoryReservation::new(broker, "ordinary incremental test"),
        )
        .unwrap();

        let output = processor
            .process_arrow(batch(&[(3, 11, ordinary_count_delta(7))]))
            .unwrap();
        let final_call = lower_call(&calls().1).unwrap();
        let encoded = output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(
            decode_state(encoded.value(0), std::slice::from_ref(&final_call))
                .unwrap()
                .values(std::slice::from_ref(&final_call)),
            vec![Some(AggregateValue::Int(7))]
        );
        assert_eq!(processor.statistics(), [0, 0]);
        assert!(processor.snapshot_key_group(0).unwrap().len() <= 32);
    }
}
