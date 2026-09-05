// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use chrono_tz::Tz;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::proto;

use super::window_aggregate::local_to_epoch;

pub(crate) fn create(
    window: &proto::WindowTableFunction,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    validate(window)?;
    let child_schema = child.schema();
    let visible_count = child_schema.fields().len().checked_sub(1).ok_or_else(|| {
        DataFusionError::Plan("Window TVF input has no selection ordinal".to_string())
    })?;
    if child_schema.field(visible_count).name() != "__streamfusion_input_row" {
        return Err(DataFusionError::Plan(
            "Window TVF selection ordinal must be the final input column".to_string(),
        ));
    }
    let index = window.time_attribute_index as usize;
    let field = child_schema
        .fields()
        .get(index)
        .filter(|_| index < visible_count)
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Window TVF time attribute index {index} is outside its input"
            ))
        })?;
    if !matches!(field.data_type(), DataType::Timestamp(_, _)) {
        return Err(DataFusionError::Plan(format!(
            "Window TVF time attribute must be a timestamp, got {}",
            field.data_type()
        )));
    }
    Ok(Arc::new(WindowTableFunctionExec::new(
        child,
        window.clone(),
    )))
}

#[derive(Debug)]
struct WindowTableFunctionExec {
    input: Arc<dyn ExecutionPlan>,
    window: proto::WindowTableFunction,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl WindowTableFunctionExec {
    fn new(input: Arc<dyn ExecutionPlan>, window: proto::WindowTableFunction) -> Self {
        let input_schema = input.schema();
        let ordinal_index = input_schema.fields().len() - 1;
        let mut fields = input_schema
            .fields()
            .iter()
            .take(ordinal_index)
            .cloned()
            .collect::<Vec<_>>();
        fields.extend(["window_start", "window_end", "window_time"].map(|name| {
            Arc::new(Field::new(
                name,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ))
        }));
        fields.push(Arc::clone(&input_schema.fields()[ordinal_index]));
        let schema = Arc::new(Schema::new(fields));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        ));
        Self {
            input,
            window,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for WindowTableFunctionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamFusionWindowTableFunctionExec")
    }
}

impl ExecutionPlan for WindowTableFunctionExec {
    fn name(&self) -> &'static str {
        "StreamFusionWindowTableFunctionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Window TVF expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children.remove(0), self.window.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&schema);
        let window = self.window.clone();
        let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
        let reservation =
            MemoryConsumer::new("StreamFusionWindowTableFunctionExec").register(&memory_pool);
        let null_row_times =
            MetricBuilder::new(&self.metrics).counter("numNullRowTimeRecordsDropped", partition);
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            let batch = batch?;
            null_row_times.add(
                batch
                    .column(window.time_attribute_index as usize)
                    .null_count(),
            );
            let output = expand_batch(batch, &window, Arc::clone(&output_schema))?;
            reservation.try_resize(output.get_array_memory_size())?;
            Ok(output)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn expand_batch(
    batch: RecordBatch,
    window: &proto::WindowTableFunction,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let shift_time_zone = if window.shift_time_zone.is_empty() {
        chrono_tz::UTC
    } else {
        window.shift_time_zone.parse::<Tz>().map_err(|error| {
            DataFusionError::Plan(format!(
                "invalid Window TVF shift time zone {}: {error}",
                window.shift_time_zone
            ))
        })?
    };
    let timestamps = batch.column(window.time_attribute_index as usize);
    let mut indices = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut assigned_windows = Vec::new();
    for row in 0..batch.num_rows() {
        let Some(epoch_millis) = timestamp_millis(timestamps, row)? else {
            continue;
        };
        let timestamp = to_window_time(epoch_millis, shift_time_zone)?;
        assign_windows_into(window, timestamp, &mut assigned_windows);
        for &(start, end) in &assigned_windows {
            indices.push(u32::try_from(row).map_err(|_| {
                DataFusionError::Execution("Window TVF batch exceeds UInt32 indexing".to_string())
            })?);
            starts.push(start);
            ends.push(end);
        }
    }
    let indices = UInt32Array::from(indices);
    let ordinal_index = batch.num_columns() - 1;
    let mut columns = batch
        .columns()
        .iter()
        .take(ordinal_index)
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
    columns.push(Arc::new(TimestampMillisecondArray::from(starts)));
    columns.push(Arc::new(TimestampMillisecondArray::from(ends.clone())));
    columns.push(Arc::new(TimestampMillisecondArray::from(
        ends.into_iter()
            .map(|end| window_time(end, shift_time_zone))
            .collect::<Result<Vec<_>>>()?,
    )));
    columns.push(take(batch.column(ordinal_index).as_ref(), &indices, None)?);
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(super) fn to_window_time(epoch_millis: i64, shift_time_zone: Tz) -> Result<i64> {
    if shift_time_zone == chrono_tz::UTC || epoch_millis == i64::MAX {
        return Ok(epoch_millis);
    }
    let instant =
        chrono::DateTime::<Utc>::from_timestamp_millis(epoch_millis).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Window TVF timestamp {epoch_millis} is outside chrono's range"
            ))
        })?;
    Ok(instant
        .with_timezone(&shift_time_zone)
        .naive_local()
        .and_utc()
        .timestamp_millis())
}

fn window_time(window_end: i64, shift_time_zone: Tz) -> Result<i64> {
    let local_millis = window_end.wrapping_sub(1);
    if shift_time_zone == chrono_tz::UTC || local_millis == i64::MAX {
        return Ok(local_millis);
    }
    let local = chrono::DateTime::<Utc>::from_timestamp_millis(local_millis)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Window TVF end {window_end} is outside chrono's range"
            ))
        })?
        .naive_utc();
    local_to_epoch(local, shift_time_zone)
}

pub(super) fn timestamp_millis(array: &ArrayRef, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
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
                / 1_000,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row)
                / 1_000_000,
        ),
        other => {
            return Err(DataFusionError::Execution(format!(
                "Window TVF expected timestamp input, got {other}"
            )));
        }
    };
    value
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Window TVF timestamp is outside Flink's millisecond range".to_string(),
            )
        })
        .map(Some)
}

fn validate(window: &proto::WindowTableFunction) -> Result<()> {
    let kind = proto::WindowKind::try_from(window.kind)
        .map_err(|_| DataFusionError::Plan(format!("unknown Window TVF kind {}", window.kind)))?;
    if kind == proto::WindowKind::Unspecified || window.size_millis <= 0 {
        return Err(DataFusionError::Plan(
            "Window TVF kind and positive size are required".to_string(),
        ));
    }
    if !window.shift_time_zone.is_empty() {
        window.shift_time_zone.parse::<Tz>().map_err(|error| {
            DataFusionError::Plan(format!(
                "invalid Window TVF shift time zone {}: {error}",
                window.shift_time_zone
            ))
        })?;
    }
    match kind {
        proto::WindowKind::CountTumble | proto::WindowKind::CountHop => Err(DataFusionError::Plan(
            "count windows are only valid for legacy group aggregation".to_string(),
        )),
        proto::WindowKind::Tumble if window.slide_or_step_millis != 0 => Err(
            DataFusionError::Plan("TUMBLE must not define a slide or step".to_string()),
        ),
        proto::WindowKind::Hop if window.slide_or_step_millis <= 0 => Err(DataFusionError::Plan(
            "HOP slide must be positive".to_string(),
        )),
        proto::WindowKind::Cumulate
            if window.slide_or_step_millis <= 0
                || window.size_millis % window.slide_or_step_millis != 0 =>
        {
            Err(DataFusionError::Plan(
                "CUMULATE size must be an integral multiple of its positive step".to_string(),
            ))
        }
        _ => Ok(()),
    }
}

pub(super) fn window_start(timestamp: i64, offset: i64, size: i64) -> i64 {
    let remainder = timestamp.wrapping_sub(offset) % size;
    if remainder < 0 {
        timestamp.wrapping_sub(remainder.wrapping_add(size))
    } else {
        timestamp.wrapping_sub(remainder)
    }
}

#[cfg(test)]
pub(super) fn assign_windows(
    window: &proto::WindowTableFunction,
    timestamp: i64,
) -> Vec<(i64, i64)> {
    let mut windows = Vec::new();
    assign_windows_into(window, timestamp, &mut windows);
    windows
}

pub(super) fn assign_windows_into(
    window: &proto::WindowTableFunction,
    timestamp: i64,
    windows: &mut Vec<(i64, i64)>,
) {
    windows.clear();
    match proto::WindowKind::try_from(window.kind).expect("validated kind") {
        proto::WindowKind::Tumble => {
            let start = window_start(timestamp, window.offset_millis, window.size_millis);
            windows.push((start, start.wrapping_add(window.size_millis)));
        }
        proto::WindowKind::Hop => {
            let slide = window.slide_or_step_millis;
            let mut start = window_start(timestamp, window.offset_millis, slide);
            let lower = timestamp.wrapping_sub(window.size_millis);
            while start > lower {
                windows.push((start, start.wrapping_add(window.size_millis)));
                start = start.wrapping_sub(slide);
            }
        }
        proto::WindowKind::Cumulate => {
            let step = window.slide_or_step_millis;
            let start = window_start(timestamp, window.offset_millis, window.size_millis);
            let last_end = start.wrapping_add(window.size_millis);
            let mut end = window_start(timestamp, window.offset_millis, step).wrapping_add(step);
            while end <= last_end {
                windows.push((start, end));
                end = end.wrapping_add(step);
            }
        }
        proto::WindowKind::Session => {
            windows.push((timestamp, timestamp.saturating_add(window.size_millis)));
        }
        proto::WindowKind::Unspecified
        | proto::WindowKind::CountTumble
        | proto::WindowKind::CountHop => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn window(
        kind: proto::WindowKind,
        size: i64,
        slide_or_step: i64,
        offset: i64,
    ) -> proto::WindowTableFunction {
        proto::WindowTableFunction {
            input: None,
            time_attribute_index: 1,
            kind: kind.into(),
            size_millis: size,
            slide_or_step_millis: slide_or_step,
            offset_millis: offset,
            partition_key_indices: Vec::new(),
            processing_time: false,
            input_schema: None,
            shift_time_zone: String::new(),
        }
    }

    #[test]
    fn assigns_each_flink_window_kind_in_flink_order() {
        assert_eq!(
            assign_windows(&window(proto::WindowKind::Tumble, 5_000, 0, 1_000), -1),
            vec![(-4_000, 1_000)]
        );
        assert_eq!(
            assign_windows(&window(proto::WindowKind::Hop, 10_000, 4_000, 0), 9_000),
            vec![(8_000, 18_000), (4_000, 14_000), (0, 10_000)]
        );
        assert_eq!(
            assign_windows(
                &window(proto::WindowKind::Cumulate, 10_000, 2_000, 0),
                4_500
            ),
            vec![(0, 6_000), (0, 8_000), (0, 10_000)]
        );
    }

    #[test]
    fn local_zoned_timestamp_assignment_uses_flinks_local_window_clock() {
        let new_york = "America/New_York".parse::<Tz>().unwrap();
        // 2026-03-08T07:30Z is 03:30 after the spring-forward gap. Flink assigns the
        // local 03:00-04:00 window rather than the epoch-aligned 07:00-08:00 window.
        let epoch = chrono::DateTime::parse_from_rfc3339("2026-03-08T07:30:00Z")
            .unwrap()
            .timestamp_millis();
        let local = to_window_time(epoch, new_york).unwrap();
        assert_eq!(
            assign_windows(&window(proto::WindowKind::Tumble, 3_600_000, 0, 0), local),
            vec![(
                chrono::NaiveDateTime::parse_from_str("2026-03-08 03:00:00", "%Y-%m-%d %H:%M:%S")
                    .unwrap()
                    .and_utc()
                    .timestamp_millis(),
                chrono::NaiveDateTime::parse_from_str("2026-03-08 04:00:00", "%Y-%m-%d %H:%M:%S")
                    .unwrap()
                    .and_utc()
                    .timestamp_millis()
            )]
        );
    }

    #[tokio::test]
    async fn expands_batches_and_drops_null_event_times() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "rowtime",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("__streamfusion_input_row", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_000),
                    None,
                    Some(5_000),
                ])),
                Arc::new(Int32Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create(&window(proto::WindowKind::Tumble, 5_000, 0, 0), input).unwrap();
        let output = collect(Arc::clone(&plan), SessionContext::new().task_ctx())
            .await
            .unwrap();

        assert_eq!(output[0].num_rows(), 2);
        assert_eq!(
            output[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 3]
        );
        assert_eq!(
            output[0]
                .column(4)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values(),
            &[4_999, 9_999]
        );
        assert_eq!(
            output[0]
                .column(5)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 2]
        );
        assert_eq!(
            plan.metrics()
                .unwrap()
                .sum_by_name("numNullRowTimeRecordsDropped")
                .unwrap()
                .as_usize(),
            1
        );
    }

    #[test]
    fn rejects_invalid_cumulate_contract() {
        assert!(validate(&window(proto::WindowKind::Cumulate, 10_000, 3_000, 0)).is_err());
    }
}
