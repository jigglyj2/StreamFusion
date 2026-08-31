// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Array, Int8Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;

use crate::exchange::{assign_key_group, encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::state::{
    KeyedState, MemoryKeyedState, RocksPluginKeyedState, StateKey, StateKeyRef, StateMutation,
};
use crate::{decode_plan, proto};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;

/// Stateful execution handle for Flink's timer-free keep-last deduplicate node.
pub(crate) struct DeduplicateProcessor {
    plan: proto::Deduplicate,
    max_parallelism: u32,
    state: Box<dyn KeyedState>,
    input_schema: Option<SchemaRef>,
    key_fields: Vec<(usize, KeyField)>,
    preencoded_key_index: Option<usize>,
    stored_row_index: Option<usize>,
    input_kind_index: Option<usize>,
    visible_count: Option<usize>,
}

impl DeduplicateProcessor {
    pub(crate) fn new(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        state_reservation: HostMemoryReservation,
    ) -> Result<Self> {
        let state = Box::new(MemoryKeyedState::new(
            first_key_group,
            last_key_group,
            state_reservation,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state)
    }

    pub(crate) fn new_rocksdb(
        serialized_plan: &[u8],
        max_parallelism: u32,
        first_key_group: u32,
        last_key_group: u32,
        plugin_path: &std::path::Path,
        database_path: &std::path::Path,
        memory_limit: usize,
    ) -> Result<Self> {
        let state = Box::new(RocksPluginKeyedState::open(
            plugin_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
        )?);
        Self::with_state(serialized_plan, max_parallelism, state)
    }

    fn with_state(
        serialized_plan: &[u8],
        max_parallelism: u32,
        state: Box<dyn KeyedState>,
    ) -> Result<Self> {
        let native_plan = decode_plan(serialized_plan)?;
        let root = native_plan
            .root
            .ok_or_else(|| DataFusionError::Plan("deduplicate plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::Deduplicate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "stateful deduplicate handle requires a Deduplicate root".to_string(),
                ));
            }
        };
        validate_plan(&plan, max_parallelism)?;
        Ok(Self {
            plan,
            max_parallelism,
            state,
            input_schema: None,
            key_fields: Vec::new(),
            preencoded_key_index: None,
            stored_row_index: None,
            input_kind_index: None,
            visible_count: None,
        })
    }

    #[cfg(test)]
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let selection = self.select_rowtime(&batch)?;
        let indices = arrow::array::UInt32Array::from(
            selection
                .ordinals
                .into_iter()
                .map(|ordinal| u32::try_from(ordinal).unwrap())
                .collect::<Vec<_>>(),
        );
        let mut columns = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
            .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
        columns.push(Arc::new(Int8Array::from(selection.row_kinds)));
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            "__streamfusion_row_kind",
            DataType::Int8,
            false,
        )));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    /// Boundary form: Java already owns the selected source rows, so only return ordinals and
    /// changelog kinds instead of gathering every visible Arrow column a second time.
    pub(crate) fn process_selection(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let selection = if self.plan.input_changelog {
            self.select_changelog(&batch)?
        } else {
            self.select_rowtime(&batch)?
        };
        let mut fields = vec![
            Field::new("__streamfusion_input_row", DataType::Int32, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(selection.ordinals)),
            Arc::new(Int8Array::from(selection.row_kinds)),
        ];
        if let Some(stored_rows) = selection.stored_rows {
            let mut builder = BinaryBuilder::new();
            for row in stored_rows {
                match row {
                    Some(row) => builder.append_value(row),
                    None => builder.append_null(),
                }
            }
            fields.push(Field::new(
                "__streamfusion_stored_row",
                DataType::Binary,
                true,
            ));
            columns.push(Arc::new(builder.finish()));
        }
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    /// Arrow-native runtime form: gather selected visible fields once and retain ordinal metadata
    /// solely for Flink's out-of-band record timestamps.
    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let selection = if self.plan.input_changelog {
            self.select_changelog(&batch)?
        } else {
            self.select_rowtime(&batch)?
        };
        if selection.ordinals.iter().any(|ordinal| *ordinal < 0) {
            return Err(DataFusionError::Execution(
                "Arrow-native deduplicate cannot materialize a previous row from opaque state"
                    .to_string(),
            ));
        }
        let indices = arrow::array::UInt32Array::from(
            selection
                .ordinals
                .iter()
                .map(|ordinal| u32::try_from(*ordinal))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| {
                    DataFusionError::Execution(
                        "deduplicate output ordinal does not fit UInt32".to_string(),
                    )
                })?,
        );
        let visible_count = self.visible_count.expect("schema prepared");
        let mut columns = batch.columns()[..visible_count]
            .iter()
            .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
            .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
        columns.push(Arc::new(Int8Array::from(selection.row_kinds)));
        columns.push(Arc::new(Int32Array::from(selection.ordinals)));
        let mut fields = batch.schema().fields()[..visible_count]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
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
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }

    fn select_rowtime(&mut self, batch: &RecordBatch) -> Result<Selection> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let stored_rows = self.stored_rows(batch)?;
        let mut state_keys = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = match self.preencoded_key_index {
                Some(index) => batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "deduplicate preencoded key column is not Arrow Binary".to_string(),
                        )
                    })?
                    .value(row)
                    .to_vec(),
                None => encode_binary_row(batch, row, &self.key_fields)?,
            };
            state_keys.push((assign_key_group(&key, self.max_parallelism), key));
        }
        let state_key_refs = state_keys
            .iter()
            .map(|(key_group, key)| StateKeyRef {
                key_group: *key_group,
                key,
            })
            .collect::<Vec<_>>();
        // RocksDB lowers this call to one multi_get. In-memory state returns borrowed values.
        let existing = self.state.get_batch(&state_key_refs)?;
        let mut staged = HashMap::<StateKeyRef<'_>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut staged_values = Vec::<Option<(usize, Vec<u8>)>>::with_capacity(batch.num_rows());
        let mut selected = Vec::with_capacity(batch.num_rows());
        let mut row_kinds = Vec::with_capacity(batch.num_rows());
        let mut output_stored_rows = self
            .plan
            .generate_update_before
            .then(|| Vec::with_capacity(batch.num_rows() * 2));

        for row in 0..batch.num_rows() {
            let order_millis = timestamp_millis(batch.column(self.plan.order_index as usize), row)?;
            let key = state_key_refs[row];
            let previous_value = staged
                .get(&key)
                .and_then(|&index| {
                    staged_values[index]
                        .as_ref()
                        .map(|(_, value)| value.as_slice())
                })
                .or_else(|| existing[row].as_deref());
            let previous_order = previous_value.map(decode_order).transpose()?;
            let previous_stored = if self.plan.generate_update_before {
                previous_value
                    .map(decode_stored_row)
                    .transpose()?
                    .map(<[u8]>::to_vec)
            } else {
                None
            };
            if previous_order.is_some_and(|previous| previous > order_millis) {
                continue;
            }
            if let Some(previous_staging) = staged.insert(key, staged_values.len()) {
                staged_values[previous_staging] = None;
            }
            let current_stored = stored_rows.map(|rows| rows.value(row));
            staged_values.push(Some((row, encode_value(order_millis, current_stored))));
            if self.plan.generate_update_before {
                if let Some(previous) = previous_stored {
                    selected.push(-1);
                    row_kinds.push(UPDATE_BEFORE);
                    output_stored_rows.as_mut().unwrap().push(Some(previous));
                }
            }
            selected.push(i32::try_from(row).map_err(|_| {
                DataFusionError::Execution("deduplicate batch exceeds UInt32 indexing".to_string())
            })?);
            row_kinds.push(if previous_order.is_none() && self.plan.generate_insert {
                INSERT
            } else {
                UPDATE_AFTER
            });
            if let Some(output) = output_stored_rows.as_mut() {
                output.push(None);
            }
        }

        drop(existing);
        drop(staged);
        drop(state_key_refs);
        let mut final_values = vec![None; batch.num_rows()];
        for (input_row, value) in staged_values.into_iter().flatten() {
            final_values[input_row] = Some(value);
        }
        let mutations = state_keys
            .into_iter()
            .zip(final_values)
            .filter_map(|((key_group, key), value)| {
                value.map(|value| StateMutation {
                    key: StateKey { key_group, key },
                    value: Some(value),
                })
            })
            .collect();
        // RocksDB lowers this call to one atomic WriteBatch.
        self.state.write_batch(mutations)?;
        Ok(Selection {
            ordinals: selected,
            row_kinds,
            stored_rows: output_stored_rows,
        })
    }

    fn select_changelog(&mut self, batch: &RecordBatch) -> Result<Selection> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let stored_rows = self.stored_rows(batch)?.ok_or_else(|| {
            DataFusionError::Execution(
                "changelog deduplicate requires opaque Flink BinaryRow bytes".to_string(),
            )
        })?;
        let input_kinds = batch
            .column(self.input_kind_index.expect("schema prepared"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "deduplicate input RowKind metadata is not Arrow Int8".to_string(),
                )
            })?;
        let state_keys = self.state_keys(batch)?;
        let state_key_refs = state_keys
            .iter()
            .map(|(key_group, key)| StateKeyRef {
                key_group: *key_group,
                key,
            })
            .collect::<Vec<_>>();
        let existing = self.state.get_batch(&state_key_refs)?;
        let mut staged = HashMap::<StateKeyRef<'_>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut staged_values =
            Vec::<Option<(usize, Option<Vec<u8>>)>>::with_capacity(batch.num_rows());
        let mut ordinals = Vec::with_capacity(batch.num_rows() * 2);
        let mut row_kinds = Vec::with_capacity(batch.num_rows() * 2);
        let mut output_stored_rows = Vec::with_capacity(batch.num_rows() * 2);

        for row in 0..batch.num_rows() {
            let key = state_key_refs[row];
            let staged_previous = staged
                .get(&key)
                .map(|&index| staged_values[index].as_ref().unwrap().1.as_deref());
            let previous = staged_previous.unwrap_or_else(|| existing[row].as_deref());
            match input_kinds.value(row) {
                INSERT | UPDATE_AFTER => {
                    let current = stored_rows.value(row);
                    if previous.is_some_and(|previous| previous == current) {
                        continue;
                    }
                    if let Some(previous) = previous {
                        if self.plan.generate_update_before {
                            ordinals.push(-1);
                            row_kinds.push(UPDATE_BEFORE);
                            output_stored_rows.push(Some(previous.to_vec()));
                        }
                        ordinals.push(i32::try_from(row).map_err(|_| {
                            DataFusionError::Execution(
                                "deduplicate batch exceeds Int32 indexing".to_string(),
                            )
                        })?);
                        row_kinds.push(UPDATE_AFTER);
                        output_stored_rows.push(None);
                    } else {
                        ordinals.push(i32::try_from(row).map_err(|_| {
                            DataFusionError::Execution(
                                "deduplicate batch exceeds Int32 indexing".to_string(),
                            )
                        })?);
                        row_kinds.push(INSERT);
                        output_stored_rows.push(None);
                    }
                    replace_staged(
                        &mut staged,
                        &mut staged_values,
                        key,
                        row,
                        Some(current.to_vec()),
                    );
                }
                UPDATE_BEFORE | DELETE => {
                    if let Some(previous) = previous {
                        ordinals.push(-1);
                        row_kinds.push(DELETE);
                        output_stored_rows.push(Some(previous.to_vec()));
                        replace_staged(&mut staged, &mut staged_values, key, row, None);
                    }
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unknown Flink RowKind byte {other}"
                    )));
                }
            }
        }

        drop(existing);
        drop(staged);
        drop(state_key_refs);
        let mut final_values = vec![None; batch.num_rows()];
        for (input_row, value) in staged_values.into_iter().flatten() {
            final_values[input_row] = Some(value);
        }
        let mutations = state_keys
            .into_iter()
            .zip(final_values)
            .filter_map(|((key_group, key), value)| {
                value.map(|value| StateMutation {
                    key: StateKey { key_group, key },
                    value,
                })
            })
            .collect();
        self.state.write_batch(mutations)?;
        Ok(Selection {
            ordinals,
            row_kinds,
            stored_rows: Some(output_stored_rows),
        })
    }

    fn state_keys(&self, batch: &RecordBatch) -> Result<Vec<(u32, Vec<u8>)>> {
        (0..batch.num_rows())
            .map(|row| {
                let key = match self.preencoded_key_index {
                    Some(index) => batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "deduplicate preencoded key column is not Arrow Binary".to_string(),
                            )
                        })?
                        .value(row)
                        .to_vec(),
                    None => encode_binary_row(batch, row, &self.key_fields)?,
                };
                Ok((assign_key_group(&key, self.max_parallelism), key))
            })
            .collect()
    }

    fn stored_rows<'a>(&self, batch: &'a RecordBatch) -> Result<Option<&'a BinaryArray>> {
        self.stored_row_index
            .map(|index| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "deduplicate stored-row metadata is not Arrow Binary".to_string(),
                        )
                    })
            })
            .transpose()
    }

    pub(crate) fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        self.state.snapshot_key_group(key_group)
    }

    pub(crate) fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        self.state.checkpoint(directory)
    }

    pub(crate) fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        self.state.restore_key_group(key_group, bytes)
    }

    fn prepare_schema(&mut self, schema: SchemaRef, column_count: usize) -> Result<()> {
        if let Some(expected) = &self.input_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "deduplicate input schema changed while the operator was running".to_string(),
                ));
            }
            return Ok(());
        }
        self.preencoded_key_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key");
        self.stored_row_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_stored_row");
        self.input_kind_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_input_row_kind");
        let input_ordinal_index = schema
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_input_row");
        let visible_count = [
            self.preencoded_key_index,
            self.stored_row_index,
            self.input_kind_index,
            input_ordinal_index,
            Some(column_count),
        ]
        .into_iter()
        .flatten()
        .min()
        .ok_or_else(|| {
            DataFusionError::Execution("deduplicate input has no visible columns".to_string())
        })?;
        if visible_count == 0 {
            return Err(DataFusionError::Execution(
                "deduplicate input has no visible columns".to_string(),
            ));
        }
        if (self.plan.input_changelog || self.plan.generate_update_before)
            && self.stored_row_index.is_none()
        {
            return Err(DataFusionError::Execution(
                "deduplicate plan requires stored BinaryRow metadata".to_string(),
            ));
        }
        if self.plan.input_changelog && self.input_kind_index.is_none() {
            return Err(DataFusionError::Execution(
                "changelog deduplicate requires input RowKind metadata".to_string(),
            ));
        }
        let order_index = self.plan.order_index as usize;
        if !self.plan.input_changelog && order_index >= visible_count {
            return Err(DataFusionError::Plan(format!(
                "deduplicate order index {order_index} is outside {visible_count} input fields"
            )));
        }
        if !self.plan.input_changelog
            && !matches!(
                schema.field(order_index).data_type(),
                DataType::Timestamp(_, _)
            )
        {
            return Err(DataFusionError::Plan(format!(
                "deduplicate ordering field must be a timestamp, got {}",
                schema.field(order_index).data_type()
            )));
        }
        if self.preencoded_key_index.is_none() {
            self.key_fields = self
                .plan
                .key_indices
                .iter()
                .map(|&index| {
                    let index = index as usize;
                    let field = schema
                        .fields()
                        .get(index)
                        .filter(|_| index < visible_count)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "deduplicate key index {index} is outside {visible_count} input fields"
                            ))
                        })?;
                    Ok((index, KeyField::from_arrow_type(field.data_type())?))
                })
                .collect::<Result<Vec<_>>>()?;
        }
        self.visible_count = Some(visible_count);
        self.input_schema = Some(schema);
        Ok(())
    }
}

struct Selection {
    ordinals: Vec<i32>,
    row_kinds: Vec<i8>,
    stored_rows: Option<Vec<Option<Vec<u8>>>>,
}

fn replace_staged<'a>(
    staged: &mut HashMap<StateKeyRef<'a>, usize, RandomState>,
    staged_values: &mut Vec<Option<(usize, Option<Vec<u8>>)>>,
    key: StateKeyRef<'a>,
    row: usize,
    value: Option<Vec<u8>>,
) {
    if let Some(previous) = staged.insert(key, staged_values.len()) {
        staged_values[previous] = None;
    }
    staged_values.push(Some((row, value)));
}

fn encode_value(order_millis: i64, row: Option<&[u8]>) -> Vec<u8> {
    let mut value = Vec::with_capacity(8 + row.map_or(0, <[u8]>::len));
    value.extend_from_slice(&order_millis.to_le_bytes());
    if let Some(row) = row {
        value.extend_from_slice(row);
    }
    value
}

fn decode_order(value: &[u8]) -> Result<i64> {
    let bytes = value.get(..8).ok_or_else(|| {
        DataFusionError::Execution(
            "deduplicate state value is shorter than its ordering field".to_string(),
        )
    })?;
    Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
}

fn decode_stored_row(value: &[u8]) -> Result<&[u8]> {
    value.get(8..).filter(|row| !row.is_empty()).ok_or_else(|| {
        DataFusionError::Execution(
            "deduplicate state has no stored BinaryRow for retraction".to_string(),
        )
    })
}

fn validate_plan(plan: &proto::Deduplicate, max_parallelism: u32) -> Result<()> {
    if plan.key_indices.is_empty() {
        return Err(DataFusionError::Plan(
            "deduplicate requires at least one key field".to_string(),
        ));
    }
    if !plan.keep_last {
        return Err(DataFusionError::Plan(
            "native deduplicate currently supports keep-last only".to_string(),
        ));
    }
    if max_parallelism == 0 || max_parallelism > 32_768 {
        return Err(DataFusionError::Plan(format!(
            "deduplicate max parallelism {max_parallelism} is outside 1..=32768"
        )));
    }
    Ok(())
}

fn timestamp_millis(array: &ArrayRef, row: usize) -> Result<i64> {
    if array.is_null(row) {
        return Err(DataFusionError::Execution(
            "deduplicate ordering timestamp must not be null".to_string(),
        ));
    }
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(row)
            .checked_mul(1_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(row)
                .div_euclid(1_000),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row)
                .div_euclid(1_000_000),
        ),
        other => {
            return Err(DataFusionError::Execution(format!(
                "deduplicate expected timestamp ordering input, got {other}"
            )));
        }
    };
    value.ok_or_else(|| {
        DataFusionError::Execution("deduplicate timestamp is outside Flink's range".to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use arrow::array::{Int64Array, StringArray};
    use prost::Message;

    fn new_processor() -> DeduplicateProcessor {
        DeduplicateProcessor::new(
            &plan(),
            128,
            0,
            127,
            HostMemoryReservation::new(
                Arc::new(TestBroker::new(1 << 30)),
                "test deduplicate state",
            ),
        )
        .unwrap()
    }

    fn plan() -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::Deduplicate(Box::new(
                    proto::Deduplicate {
                        input: Some(Box::new(proto::Operator {
                            operator: Some(proto::operator::Operator::Input(proto::Input {
                                schema: None,
                                input_index: 0,
                            })),
                        })),
                        key_indices: vec![0, 1],
                        order_index: 3,
                        keep_last: true,
                        generate_insert: true,
                        input_changelog: false,
                        generate_update_before: false,
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn batch(prices: Vec<i64>, times: Vec<i64>) -> RecordBatch {
        let count = prices.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("bidder", DataType::Int64, false),
            Field::new("auction", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
            Field::new(
                "dateTime",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("extra", DataType::Utf8, false),
            Field::new("__streamfusion_input_row", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7; count])),
                Arc::new(Int64Array::from(vec![9; count])),
                Arc::new(Int64Array::from(prices)),
                Arc::new(TimestampMillisecondArray::from(times)),
                Arc::new(StringArray::from(vec!["q18"; count])),
                Arc::new(arrow::array::Int32Array::from_iter_values(0..count as i32)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn keeps_latest_q18_row_across_batches_and_restores_raw_key_group_state() {
        let mut processor = new_processor();
        let first = processor
            .process(batch(vec![10, 20, 15], vec![1_000, 2_000, 1_500]))
            .unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(
            first
                .column(first.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[INSERT, UPDATE_AFTER]
        );

        let key_batch = batch(vec![1], vec![1]);
        let key = encode_binary_row(
            &key_batch,
            0,
            &[(0, KeyField::BigInt), (1, KeyField::BigInt)],
        )
        .unwrap();
        let key_group = assign_key_group(&key, 128);
        let snapshot = processor.snapshot_key_group(key_group).unwrap();
        let mut restored = new_processor();
        restored.restore_key_group(key_group, &snapshot).unwrap();

        let output = restored
            .process(batch(vec![99, 30], vec![1_999, 3_000]))
            .unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[30]
        );
        assert_eq!(
            output
                .column(output.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            UPDATE_AFTER
        );
    }

    #[test]
    fn arrow_runtime_gathers_only_visible_columns_and_appends_envelope_metadata() {
        let mut processor = new_processor();
        let output = processor
            .process_arrow(batch(vec![10, 20, 15], vec![1_000, 2_000, 1_500]))
            .unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.num_columns(), 7);
        assert_eq!(output.schema().field(5).name(), "__streamfusion_row_kind");
        assert_eq!(output.schema().field(6).name(), "__streamfusion_input_row");
        assert_eq!(
            output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 20]
        );
        assert_eq!(
            output
                .column(6)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 1]
        );
    }
}
