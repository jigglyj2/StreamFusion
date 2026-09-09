// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! One shared-tree owner per local window. Resource binding is explicit: capacity must be
//! the original Flink physical operator's managed-memory share, never native free bytes.

use super::*;
use crate::planner::operators::envelope::{Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use crate::planner::persistent::control::ControlEvent;
use crate::planner::persistent::unary::{InvocationState, UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use arrow::array::Int32Array;
use arrow::datatypes::{Field, Schema};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Mutex;

pub(crate) struct LocalWindowFactory(Arc<Mutex<SharedLocalWindow>>);
struct SharedLocalWindow {
    buffer: BufferedWindow,
    invocation: InvocationState,
    output_schema: Option<SchemaRef>,
    metadata_schema: SchemaRef,
    control_started: bool,
    _control_memory: HostMemoryReservation,
}

impl LocalWindowFactory {
    pub(crate) fn new(
        node: &proto::Operator,
        memory: HostMemoryReservation,
        flink_memory_bytes: usize,
        page_bytes: usize,
    ) -> Result<Self> {
        if !matches!(
            node.operator,
            Some(proto::operator::Operator::LocalWindowAggregate(_))
        ) {
            return Err(DataFusionError::Plan(
                "local window factory requires its own node".into(),
            ));
        }
        let mut plan_memory = memory.sibling("local window operator configuration");
        plan_memory.resize(crate::execution_context::operator_spec::admission(node)?)?;
        let Some(proto::operator::Operator::LocalWindowAggregate(plan)) =
            crate::execution_context::operator_spec::without_children(node).operator
        else {
            unreachable!()
        };
        let mut control_memory =
            memory.sibling("local window shared controls and envelope schemas");
        control_memory.resize(64 * 1024)?;
        let kernel = LocalWindowAggregateProcessor::from_plan(*plan, memory, plan_memory)?;
        let metadata_schema = Arc::new(Schema::new(vec![
            Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true),
            Field::new(ROW_KIND, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ]));
        Ok(Self(Arc::new(Mutex::new(SharedLocalWindow {
            buffer: BufferedWindow::new(kernel, flink_memory_bytes, page_bytes)?,
            invocation: InvocationState::Idle,
            output_schema: None,
            metadata_schema,
            control_started: false,
            _control_memory: control_memory,
        }))))
    }
}
impl PersistentOperatorFactory for LocalWindowFactory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, event: ControlEvent) -> bool {
        matches!(
            event,
            ControlEvent::Watermark(_) | ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput
        )
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !matches!(
            node.operator,
            Some(proto::operator::Operator::LocalWindowAggregate(_))
        ) || children.len() != 1
        {
            return Err(DataFusionError::Plan(
                "local window requires its own node and one native child".into(),
            ));
        }
        Ok(Arc::new(
            UnaryExec::new(self.0.clone(), children.remove(0))?.with_node_id(node.plan_node_id),
        ))
    }
}
impl UnaryBatchProcessor for SharedLocalWindow {
    const NAME: &'static str = "StreamFusionLocalWindowAggregateExec";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        let envelope = Envelope::from_schema(&input)?;
        if envelope.payload_width != self.buffer.kernel.input_schema.fields().len()
            || input.fields()[..envelope.payload_width]
                .iter()
                .zip(self.buffer.kernel.input_schema.fields())
                .any(|(a, b)| {
                    a.data_type() != b.data_type() || a.name().starts_with("__streamfusion_")
                })
        {
            return Err(DataFusionError::Plan(
                "local window native input differs from its SQL payload".into(),
            ));
        }
        if self.output_schema.is_none() {
            let mut fields = self.buffer.kernel.output_schema.fields().to_vec();
            fields.extend(self.metadata_schema.fields().iter().cloned());
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
            if kinds.null_count() != 0 || kinds.values().iter().any(|kind| *kind != INSERT) {
                return Err(DataFusionError::Execution(
                    "local window input violates its INSERT-only RowKind contract".into(),
                ));
            }
        }
        // Admit projection descriptors before sharing the payload columns with the kernel.
        self.buffer.kernel.reservation.try_grow(64 * 1024)?;
        let payload = input.project(&(0..envelope.payload_width).collect::<Vec<_>>())?;
        let output = self.buffer.push(payload)?;
        match output {
            Some(output) => self.envelope(output),
            None => Ok(RecordBatch::new_empty(self.output_schema.clone().unwrap())),
        }
    }
    fn has_pending_output(&self) -> bool {
        self.buffer.has_pending()
    }
    fn poll_pending_output(&mut self) -> Result<Option<RecordBatch>> {
        self.buffer
            .poll_pending()?
            .map(|batch| self.envelope(batch))
            .transpose()
    }
    fn poll_control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        let output = if self.control_started {
            self.buffer.poll_pending()?
        } else {
            self.control_started = true;
            self.buffer.control(event)?
        };
        match output {
            Some(output) => self.envelope(output).map(Some),
            None => {
                self.control_started = false;
                Ok(None)
            }
        }
    }
}
impl SharedLocalWindow {
    fn envelope(&self, payload: RecordBatch) -> Result<RecordBatch> {
        let rows = payload.num_rows();
        let mut memory = self.buffer.retained.sibling("local window output metadata");
        memory.resize(
            rows.checked_mul(32)
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(overflow)?,
        )?;
        let metadata = RecordBatch::try_new(
            self.metadata_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![None; rows])) as ArrayRef,
                Arc::new(Int8Array::from(vec![INSERT; rows])) as ArrayRef,
                Arc::new(Int32Array::from(vec![-1; rows])) as ArrayRef,
            ],
        )?;
        let lease = memory.split(
            metadata.get_array_memory_size(),
            "local window envelope buffers",
        )?;
        let metadata = crate::memory_pool::arrow_lease::host_batch(metadata, lease)?;
        // Payload buffers retain their existing lease. Only the new metadata gets new credit.
        let mut columns = payload.columns().to_vec();
        columns.extend(metadata.columns().iter().cloned());
        Ok(RecordBatch::try_new(
            self.output_schema.clone().unwrap(),
            columns,
        )?)
    }
}

#[cfg(test)]
mod tests;
