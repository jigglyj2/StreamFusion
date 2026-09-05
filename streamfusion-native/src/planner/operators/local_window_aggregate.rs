// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, Int8Array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use chrono_tz::Tz;
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashMap;
use prost::Message;

use super::group_aggregate::{encode_state, lower_call, AccumulatorState, Call};
use super::window_table_function::{timestamp_millis, to_window_time, window_start};
use crate::memory_pool::HostMemoryReservation;
use crate::proto;

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;

/// State-free local half of Flink's two-stage slicing window aggregate.
///
/// Every raw row contributes to exactly one base slice. The opaque partial is expanded to the
/// corresponding logical TUMBLE/HOP/CUMULATE windows only after the normal keyed exchange, which
/// keeps the local CPU and network work proportional to the input rather than to overlapping
/// logical windows.
pub(crate) struct LocalWindowAggregateProcessor {
    plan: proto::LocalWindowAggregate,
    calls: Vec<Call>,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    grouping_converter: RowConverter,
    shift_time_zone: Tz,
    reservation: HostMemoryReservation,
    output_reservation: HostMemoryReservation,
}

#[derive(Hash, PartialEq, Eq)]
struct SliceKey {
    grouping_row: Vec<u8>,
    window_start: i64,
    slice_end: i64,
}

impl LocalWindowAggregateProcessor {
    pub(crate) fn new(serialized_plan: &[u8], reservation: HostMemoryReservation) -> Result<Self> {
        let native = proto::NativePlan::decode(serialized_plan)
            .map_err(|error| DataFusionError::Plan(format!("invalid native plan: {error}")))?;
        if native.protocol_version != crate::PLAN_PROTOCOL_VERSION {
            return Err(DataFusionError::Plan(format!(
                "unsupported plan protocol version {}",
                native.protocol_version
            )));
        }
        let plan = match native.root.and_then(|operator| operator.operator) {
            Some(proto::operator::Operator::LocalWindowAggregate(plan)) => *plan,
            _ => {
                return Err(DataFusionError::Plan(
                    "native plan root is not a local window aggregate".to_string(),
                ));
            }
        };
        validate_plan(&plan)?;
        let input_schema =
            crate::planner::arrow_schema(plan.input_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan("local window aggregate requires an input schema".to_string())
            })?)?;
        let output_schema =
            crate::planner::arrow_schema(plan.output_schema.as_ref().ok_or_else(|| {
                DataFusionError::Plan(
                    "local window aggregate requires an output schema".to_string(),
                )
            })?)?;
        if output_schema.fields().len() != plan.grouping_indices.len() + 3
            || output_schema.fields()[plan.grouping_indices.len()].data_type() != &DataType::Binary
            || output_schema.fields()[plan.grouping_indices.len() + 1].data_type()
                != &DataType::Int64
            || output_schema.fields()[plan.grouping_indices.len() + 2].data_type()
                != &DataType::Int64
        {
            return Err(DataFusionError::Plan(
                "local window output must contain grouping fields, BINARY accumulator, and BIGINT window bounds"
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
                            "local window grouping index {index} is outside its input"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if !RowConverter::supports_fields(&grouping_fields) {
            return Err(DataFusionError::Plan(
                "local window grouping type is not supported by Arrow row encoding".to_string(),
            ));
        }
        let calls = plan
            .aggregate_calls
            .iter()
            .map(lower_call)
            .collect::<Result<Vec<_>>>()?;
        if plan.input_changelog && calls.iter().any(|call| !call.retractable) {
            return Err(DataFusionError::Plan(
                "changelog local window aggregate requires retractable calls".to_string(),
            ));
        }
        let shift_time_zone = if plan.shift_time_zone.is_empty() {
            chrono_tz::UTC
        } else {
            plan.shift_time_zone.parse::<Tz>().map_err(|error| {
                DataFusionError::Plan(format!(
                    "invalid local window shift time zone {}: {error}",
                    plan.shift_time_zone
                ))
            })?
        };
        let output_reservation = reservation.sibling("native local window output");
        Ok(Self {
            plan,
            calls,
            input_schema,
            output_schema,
            grouping_converter: RowConverter::new(grouping_fields)?,
            shift_time_zone,
            reservation,
            output_reservation,
        })
    }

    pub(crate) fn process_arrow(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        self.validate_batch(&batch)?;
        let estimate = batch
            .num_rows()
            .saturating_mul(192usize.saturating_add(self.calls.len().saturating_mul(64)));
        self.reservation.resize(estimate)?;
        let result = self.process_accounted(&batch);
        match result {
            Ok(output) => {
                let bytes = output.get_array_memory_size();
                self.output_reservation.resize(bytes)?;
                self.output_reservation.transfer_to_arrow(bytes)?;
                self.output_reservation.resize(0)?;
                self.reservation.resize(0)?;
                Ok(output)
            }
            Err(error) => {
                self.reservation.resize(0)?;
                Err(error)
            }
        }
    }

    fn process_accounted(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let grouping_rows = self.grouping_rows(batch)?;
        let attached_columns = self
            .plan
            .attached_window_start_index
            .zip(self.plan.attached_window_end_index)
            .map(|(start, end)| (batch.column(start as usize), batch.column(end as usize)));
        let timestamp_column = attached_columns
            .is_none()
            .then(|| batch.column(self.plan.time_attribute_index as usize));
        let slice_size = match proto::WindowKind::try_from(self.plan.kind) {
            Ok(proto::WindowKind::Tumble) => self.plan.size_millis,
            Ok(proto::WindowKind::Hop | proto::WindowKind::Cumulate) => {
                self.plan.slide_or_step_millis
            }
            _ => unreachable!("validated local window kind"),
        };
        let mut pending =
            HashMap::<SliceKey, AccumulatorState, RandomState>::with_capacity_and_hasher(
                batch.num_rows(),
                RandomState::new(),
            );
        for (row, grouping_row) in grouping_rows.into_iter().enumerate() {
            let (slice_start, slice_end) =
                if let Some((start_column, end_column)) = attached_columns {
                    let Some(start) = timestamp_millis(start_column, row)? else {
                        continue;
                    };
                    let Some(end) = timestamp_millis(end_column, row)? else {
                        continue;
                    };
                    (start, end)
                } else {
                    let Some(epoch_millis) = timestamp_millis(
                        timestamp_column.expect("direct local window has a time column"),
                        row,
                    )?
                    else {
                        continue;
                    };
                    let timestamp = to_window_time(epoch_millis, self.shift_time_zone)?;
                    let start = window_start(timestamp, self.plan.offset_millis, slice_size);
                    (start, start.wrapping_add(slice_size))
                };
            let key = SliceKey {
                grouping_row,
                window_start: slice_start,
                slice_end,
            };
            let accumulate = self.accumulates(batch, row)?;
            let entry = pending
                .entry(key)
                .or_insert_with(|| AccumulatorState::new(&self.calls));
            entry.apply(&self.calls, batch, row, accumulate)?;
        }
        let mut entries = pending
            .into_iter()
            .filter(|(_, accumulator)| accumulator.has_delta())
            .collect::<Vec<_>>();
        entries.sort_unstable_by(|(left, _), (right, _)| {
            left.slice_end
                .cmp(&right.slice_end)
                .then_with(|| left.window_start.cmp(&right.window_start))
                .then_with(|| left.grouping_row.cmp(&right.grouping_row))
        });
        let mut grouping = Vec::with_capacity(entries.len());
        let mut accumulators = Vec::with_capacity(entries.len());
        let mut window_starts = Vec::with_capacity(entries.len());
        let mut slice_ends = Vec::with_capacity(entries.len());
        for (key, accumulator) in entries {
            grouping.push(key.grouping_row);
            accumulators.push(encode_state(&accumulator));
            window_starts.push(key.window_start);
            slice_ends.push(key.slice_end);
        }
        let mut columns = if self.plan.grouping_indices.is_empty() {
            Vec::new()
        } else {
            let parser = self.grouping_converter.parser();
            self.grouping_converter
                .convert_rows(grouping.iter().map(|row| parser.parse(row)))?
        };
        columns.push(Arc::new(BinaryArray::from_iter_values(accumulators)) as ArrayRef);
        columns.push(Arc::new(Int64Array::from(window_starts)) as ArrayRef);
        columns.push(Arc::new(Int64Array::from(slice_ends)) as ArrayRef);
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }

    fn grouping_rows(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if self.plan.grouping_indices.is_empty() {
            return Ok((0..batch.num_rows()).map(|_| Vec::new()).collect());
        }
        let columns = self
            .plan
            .grouping_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index as usize)))
            .collect::<Vec<_>>();
        let rows = self.grouping_converter.convert_columns(&columns)?;
        Ok((0..batch.num_rows())
            .map(|row| rows.row(row).as_ref().to_vec())
            .collect())
    }

    fn accumulates(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        if !self.plan.input_changelog {
            return Ok(true);
        }
        let kinds = batch
            .column(self.input_schema.fields().len())
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("validated changelog metadata");
        match kinds.value(row) {
            INSERT | UPDATE_AFTER => Ok(true),
            UPDATE_BEFORE | DELETE => Ok(false),
            value => Err(DataFusionError::Execution(format!(
                "unknown local window RowKind byte {value}"
            ))),
        }
    }

    fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        let expected = self.input_schema.fields().len() + usize::from(self.plan.input_changelog);
        if batch.num_columns() != expected {
            return Err(DataFusionError::Plan(format!(
                "local window aggregate expected {expected} columns, got {}",
                batch.num_columns()
            )));
        }
        for (index, planned) in self.input_schema.fields().iter().enumerate() {
            if !planned
                .data_type()
                .equals_datatype(batch.schema().field(index).data_type())
            {
                return Err(DataFusionError::Plan(format!(
                    "local window input {index} expected {}, got {}",
                    planned.data_type(),
                    batch.schema().field(index).data_type()
                )));
            }
        }
        if self.plan.input_changelog
            && batch
                .column(self.input_schema.fields().len())
                .as_any()
                .downcast_ref::<Int8Array>()
                .is_none()
        {
            return Err(DataFusionError::Plan(
                "local window changelog metadata must be Int8".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_plan(plan: &proto::LocalWindowAggregate) -> Result<()> {
    let kind = proto::WindowKind::try_from(plan.kind)
        .map_err(|_| DataFusionError::Plan(format!("unknown local window kind {}", plan.kind)))?;
    if plan.attached_window_start_index.is_some() != plan.attached_window_end_index.is_some() {
        return Err(DataFusionError::Plan(
            "attached local window aggregate requires both window bounds".to_string(),
        ));
    }
    match kind {
        proto::WindowKind::Tumble if plan.size_millis > 0 => Ok(()),
        proto::WindowKind::Hop | proto::WindowKind::Cumulate
            if plan.size_millis > 0
                && plan.slide_or_step_millis > 0
                && plan.size_millis % plan.slide_or_step_millis == 0 =>
        {
            Ok(())
        }
        _ => Err(DataFusionError::Plan(
            "two-phase local window aggregate requires a valid slicing TUMBLE, HOP, or CUMULATE window"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use crate::planner::operators::group_aggregate::decode_state;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::{Field, Schema};

    fn logical_bigint(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
        }
    }

    fn logical_timestamp(nullable: bool) -> proto::LogicalType {
        proto::LogicalType {
            nullable,
            r#type: Some(proto::logical_type::Type::Timestamp(proto::PrecisionType {
                precision: 3,
            })),
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
            input_type: input.map(|_| logical_bigint(false)),
            output_type: Some(logical_bigint(false)),
            retractable: true,
            filter_index: None,
            distinct: false,
            accumulator_type: None,
        }
    }

    fn processor(changelog: bool) -> LocalWindowAggregateProcessor {
        let plan = proto::NativePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::LocalWindowAggregate(Box::new(
                    proto::LocalWindowAggregate {
                        input: None,
                        grouping_indices: vec![0],
                        aggregate_calls: vec![
                            call(proto::AggregateFunction::CountStar, None),
                            call(proto::AggregateFunction::Sum, Some(1)),
                        ],
                        input_changelog: changelog,
                        time_attribute_index: 2,
                        kind: proto::WindowKind::Hop as i32,
                        size_millis: 6_000,
                        slide_or_step_millis: 2_000,
                        offset_millis: 0,
                        input_schema: Some(schema(&[
                            ("key", logical_bigint(false)),
                            ("value", logical_bigint(false)),
                            ("ts", logical_timestamp(false)),
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
                                proto::Field {
                                    name: "window_start".to_string(),
                                    r#type: Some(logical_bigint(false)),
                                },
                                proto::Field {
                                    name: "slice_end".to_string(),
                                    r#type: Some(logical_bigint(false)),
                                },
                            ],
                        }),
                        shift_time_zone: "UTC".to_string(),
                        attached_window_start_index: None,
                        attached_window_end_index: None,
                    },
                ))),
            }),
        }
        .encode_to_vec();
        LocalWindowAggregateProcessor::new(
            &plan,
            HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "local window test"),
        )
        .unwrap()
    }

    fn batch(kinds: Option<Vec<i8>>) -> RecordBatch {
        let mut fields = vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
        ];
        let mut columns = vec![
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 30, 30])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![1_000, 3_000, 3_500])) as ArrayRef,
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

    fn separated_slice_changelog_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("__streamfusion_input_row_kind", DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20, 20, 5, 10])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![
                    1_000, 2_000, 2_000, 2_000, 1_000,
                ])) as ArrayRef,
                Arc::new(Int8Array::from(vec![
                    INSERT,
                    INSERT,
                    UPDATE_BEFORE,
                    UPDATE_AFTER,
                    DELETE,
                ])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn replacement_only_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("__streamfusion_input_row_kind", DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![20, 5])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![2_000, 2_000])) as ArrayRef,
                Arc::new(Int8Array::from(vec![UPDATE_BEFORE, UPDATE_AFTER])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn emits_one_opaque_partial_per_base_slice_and_cancels_retractions() {
        let mut append = processor(false);
        let output = append.process_arrow(batch(None)).unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2_000, 4_000]
        );
        let partials = output
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(
            decode_state(partials.value(0), &append.calls)
                .unwrap()
                .row_count,
            1
        );
        assert_eq!(
            decode_state(partials.value(1), &append.calls)
                .unwrap()
                .row_count,
            2
        );

        let mut retract = processor(true);
        let output = retract
            .process_arrow(batch(Some(vec![INSERT, INSERT, DELETE])))
            .unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2_000
        );
    }

    #[test]
    fn retracting_batches_preserve_the_net_partial_for_each_slice() {
        use crate::planner::operators::group_aggregate::{Accumulator, AggregateValue};

        let mut processor = processor(true);
        let output = processor
            .process_arrow(separated_slice_changelog_batch())
            .unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            4_000
        );
        let partial = decode_state(
            output
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &processor.calls,
        )
        .unwrap();
        assert_eq!(partial.row_count, 1);
        assert_eq!(
            partial.accumulators[1],
            Accumulator::Sum {
                value: Some(AggregateValue::Int(5)),
                count: 1,
            }
        );
    }

    #[test]
    fn zero_cardinality_replacement_still_emits_aggregate_deltas() {
        use crate::planner::operators::group_aggregate::{Accumulator, AggregateValue};

        let mut processor = processor(true);
        let output = processor.process_arrow(replacement_only_batch()).unwrap();
        assert_eq!(output.num_rows(), 1);
        let partial = decode_state(
            output
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &processor.calls,
        )
        .unwrap();
        assert_eq!(partial.row_count, 0);
        assert_eq!(
            partial.accumulators[1],
            Accumulator::Sum {
                value: Some(AggregateValue::Int(-15)),
                count: 0,
            }
        );
    }
}
