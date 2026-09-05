// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, Int8Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use crate::memory_pool::HostMemoryReservation;
use crate::planner::arrow_schema;
use crate::{decode_plan, proto};

use super::top_n::compare::compare_rows;

const INPUT_KIND_COLUMN: &str = "__streamfusion_input_row_kind";
const OUTPUT_KIND_COLUMN: &str = "__streamfusion_row_kind";

/// Native BatchExecRank state machine over Flink's already sorted physical input stream.
pub(crate) struct BoundedRankProcessor {
    plan: proto::BoundedRank,
    input_schema: SchemaRef,
    output_envelope_schema: SchemaRef,
    partition_ascending: Box<[bool]>,
    partition_nulls_last: Box<[bool]>,
    sort_ascending: Box<[bool]>,
    sort_nulls_last: Box<[bool]>,
    prepared_schema: Option<SchemaRef>,
    previous: Option<RecordBatch>,
    row_number: u64,
    rank: u64,
    retained: HostMemoryReservation,
    scratch: HostMemoryReservation,
    comparator_calls: u64,
    emitted_rows: u64,
}

impl BoundedRankProcessor {
    pub(crate) fn new(serialized_plan: &[u8], reservation: HostMemoryReservation) -> Result<Self> {
        let root = decode_plan(serialized_plan)?
            .root
            .ok_or_else(|| DataFusionError::Plan("bounded rank plan has no root".to_string()))?;
        let plan = match root.operator {
            Some(proto::operator::Operator::BoundedRank(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "bounded rank handle requires a BoundedRank root".to_string(),
                ));
            }
        };
        validate_plan(&plan)?;
        let input_schema = arrow_schema(plan.input_schema.as_ref().expect("validated schema"))?;
        let output_schema = arrow_schema(plan.output_schema.as_ref().expect("validated schema"))?;
        let mut output_fields = output_schema.fields().iter().cloned().collect::<Vec<_>>();
        output_fields.push(Arc::new(Field::new(
            OUTPUT_KIND_COLUMN,
            DataType::Int8,
            false,
        )));
        let output_envelope_schema = Arc::new(Schema::new(output_fields));
        let partition_ascending = vec![true; plan.partition_key_indices.len()].into_boxed_slice();
        let partition_nulls_last = vec![false; plan.partition_key_indices.len()].into_boxed_slice();
        let sort_ascending = vec![true; plan.sort_key_indices.len()].into_boxed_slice();
        let sort_nulls_last = vec![false; plan.sort_key_indices.len()].into_boxed_slice();
        Ok(Self {
            plan,
            input_schema,
            output_envelope_schema,
            partition_ascending,
            partition_nulls_last,
            sort_ascending,
            sort_nulls_last,
            prepared_schema: None,
            previous: None,
            row_number: 0,
            rank: 0,
            scratch: reservation.sibling("native bounded rank batch scratch and output"),
            retained: reservation,
            comparator_calls: 0,
            emitted_rows: 0,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.prepare_schema(batch.schema())?;
        // The input buffers remain owned by the upstream bounded sort. Rank allocates one output
        // gather plus its selection/rank vectors; charging a second copy of the input here both
        // double-counts upstream memory and rejects otherwise bounded terminal batches.
        let estimate = batch
            .get_array_memory_size()
            .saturating_add(batch.num_rows().saturating_mul(24));
        self.scratch.resize(estimate)?;
        let result = self.process_accounted(&batch);
        if result.is_err() {
            self.scratch.resize(0)?;
        }
        result
    }

    fn process_accounted(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut indices = Vec::with_capacity(batch.num_rows());
        let mut ranks = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            self.row_number = self.row_number.saturating_add(1);
            let previous = if row == 0 {
                self.previous.as_ref().map(|batch| (batch, 0))
            } else {
                Some((batch, row - 1))
            };
            let partition_changed = match previous {
                None => true,
                Some((previous, previous_row)) => {
                    self.comparator_calls = self.comparator_calls.saturating_add(1);
                    !compare_equal(
                        previous,
                        previous_row,
                        batch,
                        row,
                        &self.plan.partition_key_indices,
                        &self.partition_ascending,
                        &self.partition_nulls_last,
                    )?
                }
            };
            if partition_changed {
                self.rank = 1;
                self.row_number = 1;
            } else if let Some((previous, previous_row)) = previous {
                self.comparator_calls = self.comparator_calls.saturating_add(1);
                if !compare_equal(
                    previous,
                    previous_row,
                    batch,
                    row,
                    &self.plan.sort_key_indices,
                    &self.sort_ascending,
                    &self.sort_nulls_last,
                )? {
                    self.rank = self.row_number;
                }
            }
            if self.rank >= self.plan.rank_start && self.rank <= self.plan.rank_end {
                indices.push(u32::try_from(row).map_err(|_| {
                    DataFusionError::Execution("bounded rank batch exceeds u32 rows".to_string())
                })?);
                ranks.push(self.rank as i64);
            }
        }

        if batch.num_rows() > 0 {
            let last =
                UInt32Array::from(vec![u32::try_from(batch.num_rows() - 1).map_err(|_| {
                    DataFusionError::Execution("bounded rank batch exceeds u32 rows".to_string())
                })?]);
            let columns = batch.columns()[..self.input_schema.fields().len()]
                .iter()
                .map(|column| take(column.as_ref(), &last, None))
                .collect::<arrow::error::Result<Vec<_>>>()?;
            let previous = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
            self.retained.resize(previous.get_array_memory_size())?;
            self.previous = Some(previous);
        }

        let selection = UInt32Array::from(indices);
        let mut columns = batch.columns()[..self.input_schema.fields().len()]
            .iter()
            .map(|column| take(column.as_ref(), &selection, None))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        if self.plan.output_rank_number {
            columns.push(Arc::new(Int64Array::from(ranks)) as ArrayRef);
        }
        let kinds = take(
            batch.column(self.input_schema.fields().len()).as_ref(),
            &selection,
            None,
        )?;
        if kinds.as_any().downcast_ref::<Int8Array>().is_none() {
            return Err(DataFusionError::Execution(
                "bounded rank RowKinds are not Arrow Int8".to_string(),
            ));
        }
        columns.push(kinds);
        let output = RecordBatch::try_new(Arc::clone(&self.output_envelope_schema), columns)?;
        self.emitted_rows = self.emitted_rows.saturating_add(output.num_rows() as u64);
        let output_bytes = output.get_array_memory_size();
        self.scratch.resize(output_bytes)?;
        self.scratch.transfer_to_arrow(output_bytes)?;
        Ok(output)
    }

    pub(crate) fn statistics(&self) -> [u64; 2] {
        [self.comparator_calls, self.emitted_rows]
    }

    fn prepare_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if let Some(expected) = &self.prepared_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(DataFusionError::Execution(
                    "bounded rank input schema changed while running".to_string(),
                ));
            }
            return Ok(());
        }
        let visible = self.input_schema.fields().len();
        if schema.fields().len() != visible + 1
            || !self
                .input_schema
                .fields()
                .iter()
                .zip(&schema.fields()[..visible])
                .all(|(expected, actual)| expected.data_type() == actual.data_type())
            || schema.field(visible).name() != INPUT_KIND_COLUMN
            || schema.field(visible).data_type() != &DataType::Int8
        {
            return Err(DataFusionError::Execution(format!(
                "bounded rank Arrow input does not match its plan: expected {:?} plus RowKind, got {schema:?}",
                self.input_schema
            )));
        }
        self.prepared_schema = Some(schema);
        Ok(())
    }
}

fn compare_equal(
    left: &RecordBatch,
    left_row: usize,
    right: &RecordBatch,
    right_row: usize,
    indices: &[u32],
    ascending: &[bool],
    nulls_last: &[bool],
) -> Result<bool> {
    Ok(compare_rows(
        left, left_row, right, right_row, indices, ascending, nulls_last,
    )? == Ordering::Equal)
}

fn validate_plan(plan: &proto::BoundedRank) -> Result<()> {
    let input = plan.input_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("bounded rank is missing its input schema".to_string())
    })?;
    let output = plan.output_schema.as_ref().ok_or_else(|| {
        DataFusionError::Plan("bounded rank is missing its output schema".to_string())
    })?;
    if plan.rank_start == 0 || plan.rank_end < plan.rank_start || plan.sort_key_indices.is_empty() {
        return Err(DataFusionError::Plan(
            "bounded rank requires a non-empty ordering and valid inclusive range".to_string(),
        ));
    }
    if plan
        .partition_key_indices
        .iter()
        .chain(&plan.sort_key_indices)
        .any(|&index| index as usize >= input.fields.len())
    {
        return Err(DataFusionError::Plan(
            "bounded rank key index is outside the input schema".to_string(),
        ));
    }
    if output.fields.len() != input.fields.len() + usize::from(plan.output_rank_number) {
        return Err(DataFusionError::Plan(
            "bounded rank output schema has the wrong arity".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use arrow::array::Int32Array;
    use prost::Message;

    #[test]
    fn ranks_ties_partitions_and_physical_kinds_across_batches() {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut processor = BoundedRankProcessor::new(
            &plan(),
            HostMemoryReservation::new(broker.clone(), "bounded rank test"),
        )
        .unwrap();

        let first = processor
            .process_arrow(batch(&[0, 0, 0], &[9, 9, 8], &[0, 3, 1]))
            .unwrap();
        let second = processor
            .process_arrow(batch(&[0, 1, 1], &[7, 5, 4], &[2, 0, 3]))
            .unwrap();

        assert_eq!(values(&first), vec![(0, 8, 3, 1)]);
        assert_eq!(values(&second), vec![(1, 4, 2, 3)]);
        assert_eq!(processor.statistics(), [9, 2]);
        assert!(
            broker.reserved() > 0,
            "the retained previous row is accounted"
        );
        drop(first);
        drop(second);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn rejects_a_batch_that_exceeds_the_host_memory_budget_without_leaking() {
        let broker = Arc::new(TestBroker::new(1));
        let mut processor = BoundedRankProcessor::new(
            &plan(),
            HostMemoryReservation::new(broker.clone(), "bounded rank constrained"),
        )
        .unwrap();
        assert!(processor
            .process_arrow(batch(&[0], &[9], &[0]))
            .unwrap_err()
            .to_string()
            .contains("Flink denied"));
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }

    fn plan() -> Vec<u8> {
        let input = proto::Schema {
            fields: vec![field("partition"), field("order")],
        };
        let output = proto::Schema {
            fields: vec![
                field("partition"),
                field("order"),
                proto::Field {
                    name: "rank".to_string(),
                    r#type: Some(proto::LogicalType {
                        nullable: false,
                        r#type: Some(proto::logical_type::Type::Bigint(
                            proto::EmptyType::default(),
                        )),
                    }),
                },
            ],
        };
        proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::BoundedRank(Box::new(
                    proto::BoundedRank {
                        input: None,
                        input_schema: Some(input),
                        output_schema: Some(output),
                        partition_key_indices: vec![0],
                        sort_key_indices: vec![1],
                        rank_start: 2,
                        rank_end: 3,
                        output_rank_number: true,
                    },
                ))),
            }),
        }
        .encode_to_vec()
    }

    fn field(name: &str) -> proto::Field {
        proto::Field {
            name: name.to_string(),
            r#type: Some(proto::LogicalType {
                nullable: false,
                r#type: Some(proto::logical_type::Type::Integer(
                    proto::EmptyType::default(),
                )),
            }),
        }
    }

    fn batch(partitions: &[i32], orders: &[i32], kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "partition",
                Arc::new(Int32Array::from(partitions.to_vec())) as ArrayRef,
            ),
            (
                "order",
                Arc::new(Int32Array::from(orders.to_vec())) as ArrayRef,
            ),
            (
                INPUT_KIND_COLUMN,
                Arc::new(Int8Array::from(kinds.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn values(batch: &RecordBatch) -> Vec<(i32, i32, i64, i8)> {
        let partitions = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let orders = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ranks = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                (
                    partitions.value(row),
                    orders.value(row),
                    ranks.value(row),
                    kinds.value(row),
                )
            })
            .collect()
    }
}
