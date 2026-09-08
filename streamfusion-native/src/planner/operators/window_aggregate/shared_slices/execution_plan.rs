// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::envelope::{Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use crate::planner::persistent::{
    control::ControlEvent,
    unary::{InvocationState, UnaryBatchProcessor, UnaryExec},
    PersistentOperatorFactory,
};
use arrow::array::Int32Array;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Mutex;

pub(crate) struct SlicingWindowFactory(Arc<Mutex<SharedWindow>>);
struct SharedWindow {
    slices: SharedSlices,
    invocation: InvocationState,
    restored_watermark: Option<i64>,
    output_schema: Option<SchemaRef>,
    metadata_schema: SchemaRef,
    _configuration: HostMemoryReservation,
}

pub(crate) fn validate_node(node: &proto::Operator, max_parallelism: u32) -> Result<()> {
    let Some(proto::operator::Operator::WindowAggregate(plan)) = &node.operator else {
        return Err(DataFusionError::Plan(
            "shared window resource requires a WindowAggregate node".into(),
        ));
    };
    validate_plan(plan, max_parallelism)?;
    if (plan.kind != proto::WindowKind::Hop as i32 && plan.kind != proto::WindowKind::Tumble as i32)
        || plan.partial_accumulator_index.is_none()
        || plan.input_changelog
        || plan.processing_time
        || !(plan.shift_time_zone.is_empty() || plan.shift_time_zone == "UTC")
    {
        return Err(DataFusionError::Plan(
            "shared window requires append-only UTC event-time TUMBLE/HOP partials".into(),
        ));
    }
    let calls = plan
        .aggregate_calls
        .iter()
        .map(lower_call)
        .collect::<Result<Vec<_>>>()?;
    if GroupedMerge::new(&calls)?.is_none() {
        return Err(DataFusionError::Plan(
            "shared window requires DataFusion grouped COUNT or append-only extrema".into(),
        ));
    }
    Ok(())
}

impl SlicingWindowFactory {
    pub(crate) fn new(
        node: &proto::Operator,
        bytes: &[u8],
        binding: &proto::NativeStateBinding,
        state: Box<dyn KeyedState>,
        scratch: HostMemoryReservation,
    ) -> Result<Self> {
        validate_node(node, binding.max_parallelism)?;
        let mut configuration = scratch.sibling("shared window plan, codecs and controls");
        configuration.resize(
            crate::execution_context::operator_spec::admission(node)?.saturating_add(64 * 1024),
        )?;
        let timer_memory = scratch.sibling("shared window timers");
        let kernel = WindowAggregateProcessor::with_state(
            bytes,
            binding.max_parallelism,
            binding.first_key_group,
            binding.last_key_group,
            state,
            timer_memory,
            scratch,
        )?;
        let mut slices = SharedSlices::new(kernel)?;
        if let Some(watermark) = binding.restored_watermark {
            // A restored operator can have an empty keyed state range. Its Flink clock still
            // applies before the first input, even when there are no key-group payloads to import.
            slices.kernel.current_event_time = watermark;
            slices.restored_watermark = Some(watermark);
        }
        Ok(Self(Arc::new(Mutex::new(SharedWindow {
            slices,
            invocation: InvocationState::Idle,
            restored_watermark: binding.restored_watermark,
            output_schema: None,
            metadata_schema: Arc::new(Schema::new(vec![
                Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true),
                Field::new(INPUT_ROW, DataType::Int32, false),
            ])),
            _configuration: configuration,
        }))))
    }
}
impl PersistentOperatorFactory for SlicingWindowFactory {
    fn gauge_definitions(
        &self,
    ) -> Result<&'static [crate::planner::persistent::gauges::GaugeDefinition]> {
        use crate::planner::persistent::gauges::GaugeDefinition;
        Ok(&[
            GaugeDefinition {
                groups: &[],
                name: "numLateRecordsDropped",
                kind: proto::NativeGaugeValueKind::Int64,
                metric_kind: proto::NativeMetricKind::Counter,
                meter_name: "lateRecordsDroppedRate",
            },
            GaugeDefinition {
                groups: &[],
                name: "watermarkLatency",
                kind: proto::NativeGaugeValueKind::Int64,
                metric_kind: proto::NativeMetricKind::WatermarkLatency,
                meter_name: "",
            },
        ])
    }
    fn write_gauge_values(&self, values: &mut [i64]) -> Result<()> {
        let [late, watermark] = values else {
            return Err(DataFusionError::Execution(
                "invalid shared window metric snapshot shape".into(),
            ));
        };
        let owner = self.0.lock().map_err(|_| poisoned())?;
        *late = owner.slices.kernel.late_records_dropped as i64;
        *watermark = owner.slices.kernel.current_event_time;
        Ok(())
    }

    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, _: ControlEvent) -> bool {
        true
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "shared window requires one native child".into(),
            ));
        }
        validate_node(
            node,
            self.0
                .lock()
                .map_err(|_| poisoned())?
                .slices
                .kernel
                .max_parallelism,
        )?;
        Ok(Arc::new(
            UnaryExec::new(self.0.clone(), children.remove(0))?.with_node_id(node.plan_node_id),
        ))
    }
    fn snapshot(&self, group: u32) -> Result<crate::state::SnapshotBytes> {
        let mut owner = self.0.lock().map_err(|_| poisoned())?;
        owner.invocation.require_idle(SharedWindow::NAME)?;
        owner.slices.snapshot(group)
    }
    fn restore(&self, group: u32, bytes: &[u8]) -> Result<()> {
        let mut owner = self.0.lock().map_err(|_| poisoned())?;
        owner.invocation.require_idle(SharedWindow::NAME)?;
        let watermark = owner.restored_watermark.ok_or_else(|| DataFusionError::Plan(
            "shared window restore requires Flink's union-operator watermark in state-binding protocol 3".into()))?;
        owner.slices.restore(group, bytes, watermark)
    }
    fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        let mut owner = self.0.lock().map_err(|_| poisoned())?;
        owner.invocation.require_idle(SharedWindow::NAME)?;
        owner.slices.checkpoint(directory)
    }
}
impl UnaryBatchProcessor for SharedWindow {
    const NAME: &'static str = "StreamFusionSlicingWindowAggregateExec";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        let envelope = Envelope::from_schema(&input)?;
        let planned =
            crate::planner::arrow_schema(self.slices.kernel.plan.input_schema.as_ref().unwrap())?;
        if envelope.payload_width != planned.fields().len()
            || input.fields()[..envelope.payload_width]
                .iter()
                .zip(planned.fields())
                .any(|(a, b)| {
                    a.data_type() != b.data_type()
                        || (a.name().starts_with("__streamfusion_")
                            && !(a.name() == b.name()
                                && matches!(
                                    a.name().as_str(),
                                    "__streamfusion_accumulator"
                                        | "__streamfusion_window_start"
                                        | "__streamfusion_slice_end"
                                )))
                })
        {
            return Err(DataFusionError::Plan(
                "shared window input differs from its SQL payload".into(),
            ));
        }
        if self.output_schema.is_none() {
            let mut fields = self
                .slices
                .kernel
                .output_schema
                .as_ref()
                .unwrap()
                .fields()
                .to_vec();
            fields.insert(
                fields.len() - 1,
                self.metadata_schema.field(0).clone().into(),
            );
            fields.push(self.metadata_schema.field(1).clone().into());
            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
        Ok(self.output_schema.clone().unwrap())
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        crate::planner::operators::envelope::validate_owned_input(&input)?;
        let schema = input.schema();
        let envelope = Envelope::from_schema(&schema)?;
        if let Some(index) = schema.fields().iter().position(|field| {
            matches!(
                field.name().as_str(),
                ROW_KIND | "__streamfusion_input_row_kind"
            )
        }) {
            let kinds = input
                .column(index)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            if kinds.null_count() != 0 || kinds.values().iter().any(|&kind| kind != INSERT) {
                return Err(DataFusionError::Execution(
                    "shared window partial input must be INSERT-only".into(),
                ));
            }
        }
        let payload = input.project(&(0..envelope.payload_width).collect::<Vec<_>>())?;
        self.slices.process(&payload)?;
        Ok(RecordBatch::new_empty(self.output_schema.clone().unwrap()))
    }
    fn poll_control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        match event {
            ControlEvent::Watermark(watermark) => loop {
                let output = self.slices.advance(watermark)?;
                if output.num_rows() != 0 {
                    return self.envelope(output).map(Some);
                }
                if self.slices.next_timer().is_none_or(|next| next > watermark) {
                    return Ok(None);
                }
            },
            // Input batches have already flushed slice state. Flink owns timer serialization
            // at snapshot/checkpoint callbacks. EOF/endInput alone must not fire windows.
            ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput => {
                self.slices.require_healthy()?;
                Ok(None)
            }
        }
    }
}
impl SharedWindow {
    fn envelope(&self, payload: RecordBatch) -> Result<RecordBatch> {
        let rows = payload.num_rows();
        let mut memory = self._configuration.sibling("shared window output metadata");
        memory.resize(rows.saturating_mul(32).saturating_add(64 * 1024))?;
        let metadata = RecordBatch::try_new(
            self.metadata_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![None; rows])) as ArrayRef,
                Arc::new(Int32Array::from(vec![-1; rows])) as ArrayRef,
            ],
        )?;
        let credit = memory.split(
            metadata.get_array_memory_size(),
            "shared window timestamp and ordinal buffers",
        )?;
        let metadata = crate::memory_pool::arrow_lease::host_batch(metadata, credit)?;
        let mut columns = payload.columns().to_vec();
        columns.insert(columns.len() - 1, metadata.column(0).clone());
        columns.push(metadata.column(1).clone());
        RecordBatch::try_new(self.output_schema.clone().unwrap(), columns).map_err(Into::into)
    }
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("shared window state lock is poisoned".into())
}

#[cfg(test)]
mod tests;
