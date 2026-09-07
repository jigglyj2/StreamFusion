// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::operators::envelope::{Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use crate::planner::persistent::control::ControlEvent;
use crate::planner::persistent::unary::{UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Mutex;

pub(crate) struct LocalGroupAggregateFactory(Arc<Mutex<LocalGroupAggregateProcessor>>);

impl LocalGroupAggregateFactory {
    pub(crate) fn new(node: &proto::Operator, memory: HostMemoryReservation) -> Result<Self> {
        let Some(proto::operator::Operator::LocalGroupAggregate(plan)) = &node.operator else {
            return Err(DataFusionError::Plan(
                "local aggregate factory requires its own node".into(),
            ));
        };
        if plan.bounded_batch {
            return Err(DataFusionError::Plan(
                "shared local aggregate bounded lifecycle is not migrated".into(),
            ));
        }
        // Copy only this operator's configuration, never its child subtree or a serialized
        // sub-plan. Recursive lowering owns children once, like Comet's physical planner.
        let mut plan_memory = memory.sibling("local aggregate operator configuration");
        plan_memory.resize(crate::execution_context::operator_spec::admission(node)?)?;
        let Some(proto::operator::Operator::LocalGroupAggregate(plan)) =
            crate::execution_context::operator_spec::without_children(node).operator
        else {
            unreachable!("validated local aggregate node")
        };
        Ok(Self(Arc::new(Mutex::new(
            LocalGroupAggregateProcessor::from_plan(*plan, memory, plan_memory)?,
        ))))
    }
}

impl PersistentOperatorFactory for LocalGroupAggregateFactory {
    fn gauge_definitions(
        &self,
    ) -> Result<&'static [crate::planner::persistent::gauges::GaugeDefinition]> {
        use crate::planner::persistent::gauges::GaugeDefinition;
        use crate::proto::NativeGaugeValueKind;
        Ok(&[
            GaugeDefinition {
                groups: &[],
                name: "bundleSize",
                kind: NativeGaugeValueKind::Int32,
            },
            GaugeDefinition {
                groups: &[],
                name: "bundleRatio",
                kind: NativeGaugeValueKind::Float64,
            },
        ])
    }
    fn write_gauge_values(&self, values: &mut [i64]) -> Result<()> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        let count = processor.pending_elements as i32;
        let ratio = if processor.pending.is_empty() {
            0.0
        } else {
            f64::from(count) / processor.pending.len() as f64
        };
        values.copy_from_slice(&[i64::from(count), ratio.to_bits() as i64]);
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
        if !matches!(
            node.operator,
            Some(proto::operator::Operator::LocalGroupAggregate(_))
        ) || children.len() != 1
        {
            return Err(DataFusionError::Plan(
                "local aggregate requires its own node and one native child".into(),
            ));
        }
        Ok(Arc::new(
            UnaryExec::new(self.0.clone(), children.remove(0))?.with_node_id(node.plan_node_id),
        ))
    }
}

impl UnaryBatchProcessor for LocalGroupAggregateProcessor {
    const NAME: &'static str = "StreamFusionLocalGroupAggregateExec";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        let envelope = Envelope::from_schema(&input)?;
        if envelope.payload_width != self.input_schema.fields().len()
            || input.fields()[..envelope.payload_width]
                .iter()
                .zip(self.input_schema.fields())
                .any(|(a, b)| a.data_type() != b.data_type())
            || input.fields()[..envelope.payload_width]
                .iter()
                .any(|field| field.name().starts_with("__streamfusion_"))
        {
            return Err(DataFusionError::Plan(
                "local aggregate native input differs from its SQL payload schema".into(),
            ));
        }
        if self.key_fields.len() != self.plan.grouping_indices.len() {
            return Err(DataFusionError::Plan(
                "shared local aggregate grouping needs native Flink BinaryRow key encoding".into(),
            ));
        }
        if self.native_output_schema.is_none() {
            let mut fields = self.output_schema.fields().to_vec();
            fields.extend([
                Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true)),
                Arc::new(Field::new(ROW_KIND, DataType::Int8, false)),
                Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)),
            ]);
            self.native_output_schema = Some(Arc::new(Schema::new(fields)));
        }
        Ok(self
            .native_output_schema
            .clone()
            .expect("prepared local output"))
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        let schema = input.schema();
        let envelope = Envelope::from_schema(&schema)?;
        let kind_index = schema.fields().iter().position(|field| {
            matches!(
                field.name().as_str(),
                ROW_KIND | "__streamfusion_input_row_kind"
            )
        });
        if let Some(index) = kind_index {
            let kinds = input
                .column(index)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("validated native envelope");
            if kinds.null_count() != 0
                || kinds.values().iter().any(|kind| {
                    !(0..=3).contains(kind) || (!self.plan.input_changelog && *kind != 0)
                })
            {
                return Err(DataFusionError::Execution(
                    "local aggregate input violates its RowKind contract".into(),
                ));
            }
        } else if self.plan.input_changelog {
            return Err(DataFusionError::Execution(
                "local changelog aggregate requires native RowKind metadata".into(),
            ));
        }
        crate::planner::operators::envelope::validate_owned_input(&input)?;
        let allowance = if input.num_rows() == 0 {
            self.input_schema
                .fields()
                .len()
                .saturating_add(self.output_schema.fields().len())
                .saturating_mul(4096)
                .saturating_add(64 * 1024)
        } else {
            self.batch_admission(&input)?
        };
        self.workspace.resize(allowance)?;
        let result = (|| {
            // Admit descriptors before an Arc-only projection adapts metadata to the kernel.
            // No payload copies, serialization, Java handoff or sub-plan construction.
            let mut indices: Vec<_> = (0..envelope.payload_width).collect();
            if self.plan.input_changelog {
                indices.push(kind_index.expect("required RowKind"));
            }
            let input = input.project(&indices)?;
            self.validate_batch(&input)?;
            if input.num_rows() == 0 {
                Ok(RecordBatch::new_empty(self.output_schema.clone()))
            } else {
                self.process_accounted(input)
            }
        })();
        self.finish_native_output(result)
    }
    fn poll_control(&mut self, _: ControlEvent) -> Result<Option<RecordBatch>> {
        let result = self.drain_native_bundle();
        match result {
            Ok(Some(output)) => self.finish_native_output(Ok(output)).map(Some),
            Ok(None) => {
                self.workspace.resize(0)?;
                Ok(None)
            }
            Err(error) => self.finish_native_output(Err(error)).map(Some),
        }
    }
}

impl LocalGroupAggregateProcessor {
    fn finish_native_output(&mut self, result: Result<RecordBatch>) -> Result<RecordBatch> {
        let result = result.and_then(|output| {
            let rows = output.num_rows();
            let mut columns = output.columns().to_vec();
            // Flink's local bundle collector emits timestamp-less INSERT partials.
            columns.extend([
                Arc::new(Int64Array::from(vec![None; rows])) as ArrayRef,
                Arc::new(Int8Array::from(vec![0; rows])) as ArrayRef,
                Arc::new(Int32Array::from(vec![-1; rows])) as ArrayRef,
            ]);
            let output = RecordBatch::try_new(
                self.native_output_schema
                    .clone()
                    .expect("prepared local output"),
                columns,
            )?;
            let memory = self.workspace.split(
                output.get_array_memory_size(),
                "local aggregate native output",
            )?;
            crate::memory_pool::arrow_lease::host_batch(output, memory)
        });
        if result.is_err() {
            self.drop_failed_bundle()?;
        }
        self.workspace.resize(0)?;
        result
    }
}

fn poisoned() -> DataFusionError {
    DataFusionError::Execution("local aggregate native buffer lock is poisoned".into())
}
