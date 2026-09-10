// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::envelope::{
    self, Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND,
};
use crate::planner::persistent::{
    control::ControlEvent, unary::InvocationState, PersistentOperatorFactory,
};
use arrow::array::{ArrayRef, Int64Array};
use datafusion::physical_plan::{joins::utils::JoinFilter, ExecutionPlan};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Mutex;

mod execution_plan;
#[cfg(test)]
mod tests;

pub(crate) struct WindowJoinFactory {
    owner: Arc<Mutex<SharedWindowJoin>>,
}
struct SharedWindowJoin {
    kernel: WindowJoinProcessor,
    filter: Option<JoinFilter>,
    invocation: InvocationState,
    contract: Vec<u8>,
    first_group: u32,
    last_group: u32,
    output_schema: SchemaRef,
    metadata_schema: SchemaRef,
    _configuration: HostMemoryReservation,
}

pub(crate) fn validate_node(node: &proto::Operator, max_parallelism: u32) -> Result<()> {
    let Some(proto::operator::Operator::WindowJoin(plan)) = &node.operator else {
        return Err(invalid("shared window join requires WindowJoin"));
    };
    validate_plan(plan, max_parallelism)?;
    if !node.clear_record_timestamps {
        return Err(invalid("shared window join must clear record timestamps"));
    }
    computation::validate(plan)?;
    Ok(())
}

impl WindowJoinFactory {
    pub(crate) fn new(
        node: &proto::Operator,
        bytes: &[u8],
        binding: &proto::NativeStateBinding,
        state: Box<dyn KeyedState>,
        scratch: HostMemoryReservation,
    ) -> Result<Self> {
        validate_node(node, binding.max_parallelism)?;
        let mut configuration = scratch.sibling("shared window join plan and codecs");
        configuration.resize(
            crate::execution_context::operator_spec::admission(node)?.saturating_add(65536),
        )?;
        let timers = scratch.sibling("shared window join timers");
        let mut kernel = WindowJoinProcessor::with_state(
            bytes,
            binding.max_parallelism,
            binding.first_key_group,
            binding.last_key_group,
            state,
            timers,
            scratch,
        )?;
        // BinaryRowDataUtil.EMPTY_ROW has the eight-byte header even at arity zero.
        // Keep partition identity identical to the Flink selector and native exchange.
        kernel.empty_partition_key = vec![0; 8];
        if binding.restored_watermark.is_some() {
            return Err(invalid(
                "Flink WindowJoin restores timers, not an operator watermark",
            ));
        }
        let filter = computation::validate(&kernel.plan)?;
        let mut config = kernel.plan.clone();
        config.left_input = None;
        config.right_input = None;
        let mut contract = b"SFWF\x02".to_vec();
        contract.extend_from_slice(&Sha256::digest(config.encode_to_vec()));
        let metadata_schema = Arc::new(Schema::new(vec![
            Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true),
            Field::new(ROW_KIND, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ]));
        let mut fields = kernel
            .visible_schemas
            .iter()
            .flat_map(|s| s.fields().iter().cloned())
            .collect::<Vec<_>>();
        fields.extend(metadata_schema.fields().iter().cloned());
        let output_schema = Arc::new(Schema::new(fields));
        Ok(Self {
            owner: Arc::new(Mutex::new(SharedWindowJoin {
                kernel,
                filter,
                invocation: InvocationState::Idle,
                contract,
                first_group: binding.first_key_group,
                last_group: binding.last_key_group,
                output_schema,
                metadata_schema,
                _configuration: configuration,
            })),
        })
    }
}

impl PersistentOperatorFactory for WindowJoinFactory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, event: ControlEvent) -> bool {
        matches!(
            event,
            ControlEvent::Watermark(_) | ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput
        )
    }
    fn gauge_definitions(
        &self,
    ) -> Result<&'static [crate::planner::persistent::gauges::GaugeDefinition]> {
        use crate::planner::persistent::gauges::GaugeDefinition;
        Ok(&[
            GaugeDefinition {
                groups: &[],
                name: "leftNumLateRecordsDropped",
                kind: proto::NativeGaugeValueKind::Int64,
                metric_kind: proto::NativeMetricKind::Counter,
                meter_name: "leftLateRecordsDroppedRate",
            },
            GaugeDefinition {
                groups: &[],
                name: "rightNumLateRecordsDropped",
                kind: proto::NativeGaugeValueKind::Int64,
                metric_kind: proto::NativeMetricKind::Counter,
                meter_name: "rightLateRecordsDroppedRate",
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
        let [left, right, watermark] = values else {
            return Err(invalid("invalid window join metric snapshot shape"));
        };
        let owner = self.owner.lock().map_err(|_| poisoned())?;
        let late = owner.kernel.late_records_dropped();
        *left = late[0] as i64;
        *right = late[1] as i64;
        *watermark = owner.kernel.current_event_time;
        Ok(())
    }
    fn build(
        &self,
        node: &proto::Operator,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_node(
            node,
            self.owner
                .lock()
                .map_err(|_| poisoned())?
                .kernel
                .max_parallelism,
        )?;
        Ok(Arc::new(execution_plan::WindowJoinExec::new(
            self.owner.clone(),
            node.plan_node_id,
            children,
        )?))
    }
    fn snapshot(&self, group: u32) -> Result<crate::state::SnapshotBytes> {
        let mut owner = self.owner.lock().map_err(|_| poisoned())?;
        owner.invocation.require_idle(NAME)?;
        owner.write_contract(group)?;
        owner.kernel.snapshot_key_group(group)
    }
    fn restore(&self, group: u32, bytes: &[u8]) -> Result<()> {
        let mut owner = self.owner.lock().map_err(|_| poisoned())?;
        owner.invocation.require_idle(NAME)?;
        // Validate before mutating the backend. Empty groups still carry the plan contract.
        let mut memory = owner.kernel.state_memory();
        memory.resize(bytes.len().saturating_mul(4).saturating_add(4096))?;
        let entries = crate::state::decode_key_group_snapshot(group, bytes)?;
        if entries
            .iter()
            .find(|(key, _)| key == SHARED_STATE_KEY)
            .map(|(_, value)| value.as_slice())
            != Some(owner.contract.as_slice())
        {
            return Err(invalid(
                "shared window join checkpoint contract/version differs from the planned operator",
            ));
        }
        drop(entries);
        owner.kernel.restore_key_group(group, bytes)?;
        // Flink InternalTimerServiceImpl restores timer queues but starts its clock
        // at MIN_VALUE. Replayed records must see that clock, including late rows.
        owner.kernel.current_event_time = i64::MIN;
        Ok(())
    }
    fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        let mut owner = self.owner.lock().map_err(|_| poisoned())?;
        owner.invocation.require_idle(NAME)?;
        for group in owner.first_group..=owner.last_group {
            owner.write_contract(group)?;
        }
        owner.kernel.checkpoint(directory)
    }
}

impl SharedWindowJoin {
    fn write_contract(&mut self, group: u32) -> Result<()> {
        self.kernel.state.write_batch(vec![StateMutation {
            key: StateKey {
                key_group: group,
                key: SHARED_STATE_KEY.to_vec(),
            },
            value: Some(self.contract.clone()),
        }])
    }
    fn validate_input(&self, side: usize, schema: &SchemaRef) -> Result<()> {
        let envelope = Envelope::from_schema(schema)?;
        let planned = &self.kernel.visible_schemas[side];
        if envelope.payload_width != planned.fields().len()
            || schema.fields()[..envelope.payload_width]
                .iter()
                .zip(planned.fields())
                .any(|(a, b)| a.data_type() != b.data_type())
        {
            return Err(invalid(
                "shared window join input differs from planned SQL payload",
            ));
        }
        Ok(())
    }
    fn ingest(&mut self, side: usize, input: RecordBatch) -> Result<()> {
        envelope::validate_owned_input(&input)?;
        self.validate_input(side, &input.schema())?;
        let width = self.kernel.visible_schemas[side].fields().len();
        let kind = input.schema().index_of(ROW_KIND)?;
        let mut fields = self.kernel.visible_schemas[side].fields().to_vec();
        fields.push(Arc::new(Field::new(
            "__streamfusion_input_row_kind",
            DataType::Int8,
            false,
        )));
        let mut columns = input.columns()[..width].to_vec();
        columns.push(input.column(kind).clone());
        self.kernel.ingest_arrow(
            side,
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
        )
    }
    fn envelope(&self, payload: RecordBatch) -> Result<RecordBatch> {
        let rows = payload.num_rows();
        let mut memory = self.kernel.state_memory();
        memory.resize(rows.saturating_mul(32).saturating_add(4096))?;
        let metadata = RecordBatch::try_new(
            self.metadata_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![None; rows])) as ArrayRef,
                Arc::new(Int8Array::from(vec![INSERT; rows])),
                Arc::new(Int32Array::from(vec![-1; rows])),
            ],
        )?;
        let credit = memory.split(
            crate::memory_pool::buffer_size::batch_bytes(&metadata)?,
            "window join output envelope",
        )?;
        let metadata = crate::memory_pool::arrow_lease::host_batch(metadata, credit)?;
        let mut columns = payload.columns().to_vec();
        columns.extend(metadata.columns().iter().cloned());
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }
}

const NAME: &str = "StreamFusionWindowJoinExec";
fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("shared window join lock poisoned".into())
}
