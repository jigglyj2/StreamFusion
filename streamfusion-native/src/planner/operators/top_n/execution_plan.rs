// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::envelope::{Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use crate::planner::persistent::control::ControlEvent;
use crate::planner::persistent::gauges::GaugeDefinition;
use crate::planner::persistent::unary::{InvocationState, UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Mutex;

pub(crate) struct TopOneFactory(pub(crate) Arc<Mutex<TopNProcessor>>);

pub(crate) fn validate_node(node: &proto::Operator, max: u32) -> Result<()> {
    let Some(proto::operator::Operator::TopN(plan)) = &node.operator else {
        return Err(DataFusionError::Plan(
            "Top-1 binding requires a TopN node".into(),
        ));
    };
    validate_plan(plan, max)?;
    if plan.strategy != proto::TopNStrategy::AppendFast as i32
        || plan.rank_start != 1
        || plan.rank_end != Some(1)
        || plan.variable_rank_end_index.is_some()
        || plan.rank_type != proto::TopNRankType::RowNumber as i32
        || plan.bounded_final_output
        || plan.physical_input_semantics
        || plan.state_ttl_millis != 0
        || plan.sort_key_indices.is_empty()
    {
        return Err(DataFusionError::Plan("shared Top-1 requires append-only ROW_NUMBER range [1,1], explicit ordering and disabled state TTL".into()));
    }
    let input = arrow_schema(plan.input_schema.as_ref().expect("validated"))?;
    let output = arrow_schema(plan.output_schema.as_ref().expect("validated"))?;
    validate_output_schema(plan, &input, &output)?;
    if crate::planner::operators::sortable_state::SortKeys::new(
        &input,
        &plan.sort_key_indices,
        &plan.sort_ascending,
        &plan.sort_nulls_last,
    )?
    .is_none()
    {
        return Err(DataFusionError::Plan(
            "shared Top-1 requires Flink-compatible Arrow row sort keys".into(),
        ));
    }
    Ok(())
}

impl PersistentOperatorFactory for TopOneFactory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, _event: ControlEvent) -> bool {
        true
    }
    fn gauge_definitions(&self) -> Result<&'static [GaugeDefinition]> {
        const METRICS: &[GaugeDefinition] = &[
            GaugeDefinition {
                groups: &[],
                name: "topn.invalidTopSize",
                metric_kind: proto::NativeMetricKind::Counter,
                meter_name: "",
                kind: proto::NativeGaugeValueKind::Int64,
            },
            GaugeDefinition {
                groups: &[],
                name: "topn.cache.hitRate",
                metric_kind: proto::NativeMetricKind::Gauge,
                meter_name: "",
                kind: proto::NativeGaugeValueKind::Float64,
            },
            GaugeDefinition {
                groups: &[],
                name: "topn.cache.size",
                metric_kind: proto::NativeMetricKind::Gauge,
                meter_name: "",
                kind: proto::NativeGaugeValueKind::Int64,
            },
        ];
        Ok(METRICS)
    }
    fn write_gauge_values(&self, values: &mut [i64]) -> Result<()> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        // Flink AbstractTopNFunction captures request/hit counts and FastTop1Helper's empty
        // cache size at registration. They remain 1.0 and 0 for this physical function.
        values.copy_from_slice(&[
            processor.invalid_top_sizes as i64,
            1.0f64.to_bits() as i64,
            0,
        ]);
        Ok(())
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_node(node, self.0.lock().map_err(|_| poisoned())?.max_parallelism)?;
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "Top-1 requires one native child".into(),
            ));
        }
        Ok(Arc::new(
            UnaryExec::new(self.0.clone(), children.remove(0))?.with_node_id(node.plan_node_id),
        ))
    }
    fn snapshot(&self, group: u32) -> Result<crate::state::SnapshotBytes> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        processor.invocation.require_idle("Top-1 snapshot")?;
        processor.snapshot_key_group(group)
    }
    fn restore(&self, group: u32, bytes: &[u8]) -> Result<()> {
        let mut processor = self.0.lock().map_err(|_| poisoned())?;
        processor.invocation.require_idle("Top-1 restore")?;
        processor.restore_key_group(group, bytes)
    }
    fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        processor.invocation.require_idle("Top-1 checkpoint")?;
        processor.checkpoint(directory)
    }
}

impl UnaryBatchProcessor for TopNProcessor {
    const NAME: &'static str = "StreamFusionTopOneExec";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        let envelope = Envelope::from_schema(input.as_ref())?;
        if envelope.payload_width != self.input_schema.fields().len() {
            return Err(DataFusionError::Plan(
                "Top-1 native payload differs from its plan".into(),
            ));
        }
        let renamed = legacy_schema(&input, &self.input_schema)?;
        self.prepare_schema(renamed)?;
        let mut fields = self.output_schema.fields().to_vec();
        if let Ok(index) = input.index_of(OWNED_TIMESTAMP_V1) {
            fields.push(input.fields()[index].clone());
        }
        fields.push(Arc::new(Field::new(ROW_KIND, DataType::Int8, false)));
        fields.push(Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)));
        let schema = Arc::new(Schema::new(fields));
        self.native_schema = Some(schema.clone());
        Ok(schema)
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        crate::planner::operators::envelope::validate_owned_input(&input)?;
        let kind = input.schema().index_of(ROW_KIND)?;
        let kinds = input
            .column(kind)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| DataFusionError::Execution("Top-1 RowKind must be Int8".into()))?;
        if kinds.null_count() != 0 || kinds.values().iter().any(|&kind| kind != INSERT) {
            return Err(DataFusionError::Execution(
                "shared Top-1 requires INSERT-only input".into(),
            ));
        }
        let input = RecordBatch::try_new(
            legacy_schema(&input.schema(), &self.input_schema)?,
            input.columns().to_vec(),
        )?;
        self.prepare_schema(input.schema())?;
        let payload = input.columns()[..self.input_schema.fields().len()]
            .iter()
            .try_fold(0usize, |bytes, array| {
                Ok::<_, DataFusionError>(
                    bytes.saturating_add(array.to_data().get_slice_memory_size()?),
                )
            })?;
        let base = payload
            .saturating_mul(3)
            .saturating_add(input.num_rows().saturating_mul(512))
            .saturating_add(64 << 10);
        self.scratch_reservation.resize(base)?;
        let result = (|| {
            let output = if input.num_rows() == 0 {
                RecordBatch::new_empty(self.native_schema.clone().expect("prepared"))
            } else {
                self.process_arrow_accounted(input, 0, base)?
            };
            let memory = self
                .scratch_reservation
                .split(output.get_array_memory_size(), "Top-1 native output")?;
            crate::memory_pool::arrow_lease::host_batch(output, memory)
        })();
        self.scratch_reservation.resize(0)?;
        result
    }
    fn poll_control(&mut self, _event: ControlEvent) -> Result<Option<RecordBatch>> {
        // State changes are already committed once per batch. Invocation EOF, watermark,
        // pre-barrier and end-input do not generate another Top-1 changelog transition.
        Ok(None)
    }
}

fn legacy_schema(input: &SchemaRef, payload: &SchemaRef) -> Result<SchemaRef> {
    let mut fields = input.fields().to_vec();
    for (index, expected) in payload.fields().iter().enumerate() {
        let actual = fields
            .get(index)
            .ok_or_else(|| DataFusionError::Plan("Top-1 native payload is truncated".into()))?;
        if !fields_compatible(expected, actual, false) {
            return Err(DataFusionError::Plan(
                "Top-1 native column type differs from its SQL plan".into(),
            ));
        }
        // DataFusion Calc assigns projection labels. SQL positions/types define this contract;
        // rebinding schema labels changes no Arrow payload or buffer ownership.
        fields[index] = Arc::new(actual.as_ref().clone().with_name(expected.name()));
    }
    let kind = input.index_of(ROW_KIND)?;
    if input.fields()[..kind].iter().any(|field| {
        matches!(
            field.name().as_str(),
            INPUT_KIND_COLUMN | PREENCODED_KEY_COLUMN
        )
    }) {
        return Err(DataFusionError::Plan(
            "Top-1 payload conflicts with native metadata".into(),
        ));
    }
    fields[kind] = Arc::new(fields[kind].as_ref().clone().with_name(INPUT_KIND_COLUMN));
    Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn with_envelope(
    output: RecordBatch,
    input: &RecordBatch,
    triggering_rows: &[u32],
) -> Result<RecordBatch> {
    if output.num_rows() != triggering_rows.len() {
        return Err(DataFusionError::Execution(
            "Top-1 output lost its triggering input positions".into(),
        ));
    }
    let selection = UInt32Array::from(triggering_rows.to_vec());
    let width = output.num_columns() - 1;
    let mut fields = output.schema().fields()[..width].to_vec();
    let mut columns = output.columns()[..width].to_vec();
    if let Ok(index) = input.schema().index_of(OWNED_TIMESTAMP_V1) {
        fields.push(input.schema().fields()[index].clone());
        columns.push(take(input.column(index), &selection, None)?);
    }
    fields.push(output.schema().fields()[width].clone());
    columns.push(output.column(width).clone());
    let ordinal = input.schema().index_of(INPUT_ROW)?;
    fields.push(input.schema().fields()[ordinal].clone());
    columns.push(take(input.column(ordinal), &selection, None)?);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(Into::into)
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("Top-1 state lock poisoned".into())
}

#[cfg(test)]
mod tests;
