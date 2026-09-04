// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int8Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;
use prost::Message;

use super::group_aggregate::{
    encode_state, lower_call, sort_flink_hashmap_keys, AccumulatorState, Call,
};
use crate::exchange::{encode_binary_row, KeyField};
use crate::memory_pool::HostMemoryReservation;
use crate::proto;

/// Flink-compatible local bundle aggregation with no persistent keyed state.
///
/// The output is intentionally a native internal contract: grouping columns followed by one
/// canonical opaque accumulator. The paired native global stage consumes it after the normal
/// Flink-owned exchange, avoiding Java interpretation of aggregate state and retaining one Arrow
/// payload throughout the accelerated portion of each task.
pub(crate) struct LocalGroupAggregateProcessor {
    plan: proto::LocalGroupAggregate,
    calls: Vec<Call>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    grouping_converter: RowConverter,
    key_fields: Vec<(usize, KeyField)>,
    pending: HashMap<Vec<u8>, PendingLocal, RandomState>,
    pending_order: Vec<Vec<u8>>,
    pending_elements: usize,
    pending_reservation: HostMemoryReservation,
    output_reservation: HostMemoryReservation,
}

struct PendingLocal {
    grouping_row: Vec<u8>,
    accumulator: AccumulatorState,
}

impl LocalGroupAggregateProcessor {
    pub(crate) fn new(plan_bytes: &[u8], reservation: HostMemoryReservation) -> Result<Self> {
        let native = proto::NativePlan::decode(plan_bytes)
            .map_err(|error| DataFusionError::Plan(format!("invalid native plan: {error}")))?;
        if native.protocol_version != crate::PLAN_PROTOCOL_VERSION {
            return Err(DataFusionError::Plan(format!(
                "unsupported plan protocol version {}",
                native.protocol_version
            )));
        }
        let plan = match native.root.and_then(|operator| operator.operator) {
            Some(proto::operator::Operator::LocalGroupAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "native plan root is not a local group aggregate".to_string(),
                ));
            }
        };
        if plan.mini_batch_size == 0 || plan.mini_batch_size > usize::MAX as u64 {
            return Err(DataFusionError::Plan(
                "local group aggregate requires a positive mini-batch size that fits usize"
                    .to_string(),
            ));
        }
        let input_schema =
            crate::planner::arrow_schema(plan.input_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("local group aggregate requires an input schema".to_string())
            })?)?;
        let output_schema =
            crate::planner::arrow_schema(plan.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("local group aggregate requires an output schema".to_string())
            })?)?;
        if output_schema.fields().len() != plan.grouping_indices.len() + 1
            || output_schema
                .fields()
                .last()
                .is_none_or(|field| field.data_type() != &arrow::datatypes::DataType::Binary)
        {
            return Err(DataFusionError::Plan(
                "local group aggregate output must contain grouping fields and one BINARY accumulator"
                    .to_string(),
            ));
        }
        let grouping_fields = plan
            .grouping_indices
            .iter()
            .map(|&index| {
                input_schema
                    .fields()
                    .get(index as usize)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "local group aggregate grouping index {index} is outside its input"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if !RowConverter::supports_fields(&grouping_fields) {
            return Err(DataFusionError::Plan(
                "local group aggregate grouping type is not supported by Arrow row encoding"
                    .to_string(),
            ));
        }
        let mut key_fields = Vec::with_capacity(plan.grouping_indices.len());
        let mut native_key_supported = true;
        for &index in &plan.grouping_indices {
            let index = index as usize;
            let field = input_schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "local group aggregate grouping index {index} is outside its input"
                ))
            })?;
            match KeyField::from_arrow_type(field.data_type()) {
                Ok(field) if native_key_supported => key_fields.push((index, field)),
                Ok(_) => {}
                Err(_) => {
                    native_key_supported = false;
                    key_fields.clear();
                }
            }
        }
        let calls = plan
            .aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        let output_reservation = reservation.sibling("native local group aggregate output");
        Ok(Self {
            plan,
            calls,
            input_schema,
            output_schema,
            grouping_converter: RowConverter::new(grouping_fields)?,
            key_fields,
            pending: HashMap::with_hasher(RandomState::new()),
            pending_order: Vec::new(),
            pending_elements: 0,
            pending_reservation: reservation,
            output_reservation,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.validate_batch(&batch)?;
        let grouping_rows = self.encode_grouping_rows(&batch)?;
        let mut output_keys = Vec::new();
        let mut output_accumulators = Vec::new();
        let trigger = self.plan.mini_batch_size as usize;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let length = (trigger - self.pending_elements).min(batch.num_rows() - offset);
            for row in offset..offset + length {
                let key = self.key(&batch, row)?;
                let accumulate = self.accumulates(&batch, row)?;
                let pending = match self.pending.entry(key) {
                    hashbrown::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        self.pending_order.push(entry.key().clone());
                        entry.insert(PendingLocal {
                            grouping_row: grouping_rows
                                .as_ref()
                                .map_or_else(Vec::new, |rows| rows.row(row).as_ref().to_vec()),
                            accumulator: AccumulatorState::new(&self.calls),
                        })
                    }
                };
                pending
                    .accumulator
                    .apply(&self.calls, &batch, row, accumulate)?;
            }
            self.pending_elements += length;
            offset += length;
            if self.pending_elements == trigger {
                self.drain_pending(&mut output_keys, &mut output_accumulators);
            }
        }
        self.resize_reservation()?;
        self.output_batch(output_keys, output_accumulators)
    }

    pub(crate) fn finish_bundle(&mut self) -> Result<RecordBatch> {
        let mut output_keys = Vec::new();
        let mut output_accumulators = Vec::new();
        self.drain_pending(&mut output_keys, &mut output_accumulators);
        self.pending_reservation.resize(0)?;
        self.output_batch(output_keys, output_accumulators)
    }

    pub(crate) fn pending_element_count(&self) -> usize {
        self.pending_elements
    }

    pub(crate) fn pending_key_count(&self) -> usize {
        self.pending.len()
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        let visible = self.input_schema.fields().len();
        let preencoded = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key");
        let expected =
            visible + usize::from(self.plan.input_changelog) + usize::from(preencoded.is_some());
        if batch.num_columns() != expected {
            return Err(DataFusionError::Plan(format!(
                "local group aggregate expected {expected} input columns but received {}",
                batch.num_columns()
            )));
        }
        let batch_schema = batch.schema();
        for (index, expected) in self.input_schema.fields().iter().enumerate() {
            let actual = batch_schema.field(index);
            if actual.data_type() != expected.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "local group aggregate input field {index} has type {} instead of {}",
                    actual.data_type(),
                    expected.data_type()
                )));
            }
        }
        if self.plan.input_changelog
            && batch
                .column(visible)
                .as_any()
                .downcast_ref::<Int8Array>()
                .is_none()
        {
            return Err(DataFusionError::Plan(
                "local group aggregate changelog metadata must be Int8".to_string(),
            ));
        }
        if let Some(index) = preencoded {
            if batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .is_none()
            {
                return Err(DataFusionError::Plan(
                    "local group aggregate preencoded key metadata must be Binary".to_string(),
                ));
            }
        } else if self.key_fields.len() != self.plan.grouping_indices.len() {
            return Err(DataFusionError::Plan(
                "local group aggregate requires Flink BinaryRow key metadata for this grouping type"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        if let Some(index) = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "__streamfusion_key")
        {
            let keys = batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("preencoded keys were validated");
            if keys.is_null(row) {
                return Err(DataFusionError::Execution(
                    "local group aggregate preencoded key may not be null".to_string(),
                ));
            }
            Ok(keys.value(row).to_vec())
        } else {
            Ok(encode_binary_row(batch, row, &self.key_fields)?)
        }
    }

    fn encode_grouping_rows(&self, batch: &RecordBatch) -> Result<Option<Rows>> {
        if self.plan.grouping_indices.is_empty() {
            return Ok(None);
        }
        let columns = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index as usize)))
            .collect::<Vec<_>>();
        Ok(Some(self.grouping_converter.convert_columns(&columns)?))
    }

    fn accumulates(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        if !self.plan.input_changelog {
            return Ok(true);
        }
        let kinds = batch
            .column(self.input_schema.fields().len())
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("changelog metadata was validated");
        if kinds.is_null(row) {
            return Err(DataFusionError::Execution(
                "local group aggregate row kind cannot be null".to_string(),
            ));
        }
        match kinds.value(row) {
            0 | 2 => Ok(true),
            1 | 3 => Ok(false),
            kind => Err(DataFusionError::Execution(format!(
                "unknown local group aggregate row kind {kind}"
            ))),
        }
    }

    fn drain_pending(&mut self, keys: &mut Vec<Vec<u8>>, accumulators: &mut Vec<Vec<u8>>) {
        let mut order = std::mem::take(&mut self.pending_order);
        sort_flink_hashmap_keys(&mut order, Vec::as_slice);
        for key in order {
            let pending = self
                .pending
                .remove(&key)
                .expect("local aggregate order and map remain synchronized");
            keys.push(pending.grouping_row);
            accumulators.push(encode_state(&pending.accumulator));
        }
        self.pending_elements = 0;
    }

    fn resize_reservation(&mut self) -> Result<()> {
        let map_bytes = self
            .pending
            .capacity()
            .saturating_mul(std::mem::size_of::<(Vec<u8>, PendingLocal)>().saturating_add(16));
        let order_bytes = self
            .pending_order
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>());
        let dynamic = self.pending.iter().fold(0usize, |bytes, (key, pending)| {
            bytes
                .saturating_add(key.capacity().saturating_mul(2))
                .saturating_add(pending.grouping_row.capacity())
                .saturating_add(pending.accumulator.estimated_dynamic_bytes())
        });
        self.pending_reservation.resize(
            map_bytes
                .saturating_add(order_bytes)
                .saturating_add(dynamic),
        )
    }

    fn output_batch(
        &mut self,
        keys: Vec<Vec<u8>>,
        accumulators: Vec<Vec<u8>>,
    ) -> Result<RecordBatch> {
        let mut columns = if self.plan.grouping_indices.is_empty() {
            Vec::new()
        } else {
            let parser = self.grouping_converter.parser();
            self.grouping_converter
                .convert_rows(keys.iter().map(|key| parser.parse(key)))?
        };
        columns.push(Arc::new(BinaryArray::from_iter_values(accumulators)) as ArrayRef);
        let output = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)?;
        let bytes = output.get_array_memory_size();
        self.output_reservation.resize(bytes)?;
        self.output_reservation.transfer_to_arrow(bytes)?;
        self.output_reservation.resize(0)?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use crate::planner::operators::group_aggregate::decode_state;
    use arrow::array::{Int64Array, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn logical_bigint(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
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

    fn call(function: proto::AggregateFunction, input: Option<u32>) -> proto::AggregateCall {
        proto::AggregateCall {
            function: function as i32,
            input_index: input,
            input_type: input.map(|_| logical_bigint(true)),
            output_type: Some(logical_bigint(
                function != proto::AggregateFunction::CountStar,
            )),
            retractable: true,
            filter_index: None,
            distinct: false,
            accumulator_type: None,
        }
    }

    fn plan(size: u64, changelog: bool) -> Vec<u8> {
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                operator: Some(proto::operator::Operator::LocalGroupAggregate(Box::new(
                    proto::LocalGroupAggregate {
                        input: None,
                        grouping_indices: vec![0],
                        aggregate_calls: vec![
                            call(proto::AggregateFunction::CountStar, None),
                            call(proto::AggregateFunction::Sum, Some(1)),
                        ],
                        input_changelog: changelog,
                        mini_batch_size: size,
                        input_schema: Some(schema(&[
                            ("key", logical_bigint(false)),
                            ("value", logical_bigint(true)),
                        ])),
                        output_schema: Some(proto::Schema {
                            fields: vec![
                                proto::Field {
                                    name: "key".to_string(),
                                    r#type: Some(logical_bigint(false)),
                                },
                                proto::Field {
                                    name: "accumulator".to_string(),
                                    r#type: Some(proto::LogicalType {
                                        nullable: false,
                                        r#type: Some(proto::logical_type::Type::Binary(
                                            proto::EmptyType {},
                                        )),
                                    }),
                                },
                            ],
                        }),
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn processor(size: u64, changelog: bool) -> LocalGroupAggregateProcessor {
        LocalGroupAggregateProcessor::new(
            &plan(size, changelog),
            HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "local aggregate test"),
        )
        .unwrap()
    }

    fn batch(keys: Vec<i64>, values: Vec<i64>, kinds: Option<Vec<i8>>) -> RecordBatch {
        let mut fields = vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ];
        let mut columns = vec![
            Arc::new(Int64Array::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
        ];
        if let Some(kinds) = kinds {
            fields.push(Field::new(
                "__streamfusion_input_row_kind",
                DataType::Int8,
                false,
            ));
            columns.push(Arc::new(Int8Array::from(kinds)) as ArrayRef);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn emits_one_opaque_delta_per_key_at_exact_bundle_boundaries() {
        let mut processor = processor(3, false);
        assert_eq!(
            processor
                .process_arrow(batch(vec![1, 1], vec![10, 20], None))
                .unwrap()
                .num_rows(),
            0
        );
        let output = processor
            .process_arrow(batch(vec![2, 1], vec![5, 7], None))
            .unwrap();
        assert_eq!(output.num_rows(), 2);
        let keys = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.values(), &[2, 1]);
        let encoded = output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let calls = processor.calls.clone();
        let first = decode_state(encoded.value(1), &calls).unwrap();
        assert_eq!(first.row_count, 2);
        assert_eq!(
            first.values(&calls)[1],
            Some(super::super::group_aggregate::AggregateValue::Int(30))
        );
        assert_eq!(processor.pending_element_count(), 1);
        assert_eq!(processor.finish_bundle().unwrap().num_rows(), 1);
    }

    #[test]
    fn preserves_negative_retraction_deltas_for_the_global_stage() {
        let mut processor = processor(10, true);
        processor
            .process_arrow(batch(vec![1], vec![7], Some(vec![3])))
            .unwrap();
        let output = processor.finish_bundle().unwrap();
        let encoded = output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let state = decode_state(encoded.value(0), &processor.calls).unwrap();
        assert_eq!(state.row_count, -1);
        assert_eq!(
            state.values(&processor.calls)[1],
            Some(super::super::group_aggregate::AggregateValue::Int(-7))
        );
    }

    #[test]
    fn accounts_pending_and_output_memory_through_the_host_broker() {
        let broker = Arc::new(TestBroker::new(1 << 20));
        let reservation =
            HostMemoryReservation::new(broker.clone(), "local aggregate accounting test");
        let mut processor =
            LocalGroupAggregateProcessor::new(&plan(10, false), reservation).unwrap();

        let empty = processor
            .process_arrow(batch(vec![1, 1, 2], vec![10, 20, 30], None))
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert!(broker.reserved() > 0, "pending hash state must be reserved");

        let output = processor.finish_bundle().unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            broker.reserved(),
            0,
            "Arrow owns the transferred output memory"
        );
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}
