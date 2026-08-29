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
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::proto;

pub(crate) fn create(
    window: &proto::WindowTableFunction,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    validate(window)?;
    let index = window.time_attribute_index as usize;
    let field = child.schema().fields().get(index).cloned().ok_or_else(|| {
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
}

impl WindowTableFunctionExec {
    fn new(input: Arc<dyn ExecutionPlan>, window: proto::WindowTableFunction) -> Self {
        let mut fields = input.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.extend(["window_start", "window_end", "window_time"].map(|name| {
            Arc::new(Field::new(
                name,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ))
        }));
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
        let stream = self
            .input
            .execute(partition, context)?
            .map(move |batch| expand_batch(batch?, &window, Arc::clone(&output_schema)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn expand_batch(
    batch: RecordBatch,
    window: &proto::WindowTableFunction,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let timestamps = batch.column(window.time_attribute_index as usize);
    let mut indices = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for row in 0..batch.num_rows() {
        let Some(timestamp) = timestamp_millis(timestamps, row)? else {
            continue;
        };
        for (start, end) in assign_windows(window, timestamp) {
            indices.push(u32::try_from(row).map_err(|_| {
                DataFusionError::Execution("Window TVF batch exceeds UInt32 indexing".to_string())
            })?);
            starts.push(start);
            ends.push(end);
        }
    }
    let indices = UInt32Array::from(indices);
    let mut columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
    columns.push(Arc::new(TimestampMillisecondArray::from(starts)));
    columns.push(Arc::new(TimestampMillisecondArray::from(ends.clone())));
    columns.push(Arc::new(TimestampMillisecondArray::from(
        ends.into_iter()
            .map(|end| end.wrapping_sub(1))
            .collect::<Vec<_>>(),
    )));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn timestamp_millis(array: &ArrayRef, row: usize) -> Result<Option<i64>> {
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
    match kind {
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

fn window_start(timestamp: i64, offset: i64, size: i64) -> i64 {
    let remainder = timestamp.wrapping_sub(offset) % size;
    if remainder < 0 {
        timestamp.wrapping_sub(remainder.wrapping_add(size))
    } else {
        timestamp.wrapping_sub(remainder)
    }
}

fn assign_windows(window: &proto::WindowTableFunction, timestamp: i64) -> Vec<(i64, i64)> {
    match proto::WindowKind::try_from(window.kind).expect("validated kind") {
        proto::WindowKind::Tumble => {
            let start = window_start(timestamp, window.offset_millis, window.size_millis);
            vec![(start, start.wrapping_add(window.size_millis))]
        }
        proto::WindowKind::Hop => {
            let slide = window.slide_or_step_millis;
            let mut start = window_start(timestamp, window.offset_millis, slide);
            let lower = timestamp.wrapping_sub(window.size_millis);
            let mut windows = Vec::new();
            while start > lower {
                windows.push((start, start.wrapping_add(window.size_millis)));
                start = start.wrapping_sub(slide);
            }
            windows
        }
        proto::WindowKind::Cumulate => {
            let step = window.slide_or_step_millis;
            let start = window_start(timestamp, window.offset_millis, window.size_millis);
            let last_end = start.wrapping_add(window.size_millis);
            let mut end = window_start(timestamp, window.offset_millis, step).wrapping_add(step);
            let mut windows = Vec::new();
            while end <= last_end {
                windows.push((start, end));
                end = end.wrapping_add(step);
            }
            windows
        }
        proto::WindowKind::Unspecified => unreachable!(),
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

    #[tokio::test]
    async fn expands_batches_and_drops_null_event_times() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "rowtime",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
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
            ],
        )
        .unwrap();
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let output = collect(
            create(&window(proto::WindowKind::Tumble, 5_000, 0, 0), input).unwrap(),
            SessionContext::new().task_ctx(),
        )
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
    }

    #[test]
    fn rejects_invalid_cumulate_contract() {
        assert!(validate(&window(proto::WindowKind::Cumulate, 10_000, 3_000, 0)).is_err());
    }
}
