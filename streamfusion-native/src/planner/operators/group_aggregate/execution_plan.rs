// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::planner::operators::envelope::{Envelope, INPUT_ROW, OWNED_TIMESTAMP_V1};
use crate::planner::persistent::control::ControlEvent;
use crate::planner::persistent::unary::{UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Mutex;

pub(crate) struct GroupAggregateFactory(pub(crate) Arc<Mutex<GroupAggregateProcessor>>);
impl PersistentOperatorFactory for GroupAggregateFactory {
    fn gauge_definitions(
        &self,
    ) -> Result<&'static [crate::planner::persistent::gauges::GaugeDefinition]> {
        use crate::planner::persistent::gauges::GaugeDefinition;
        use crate::proto::NativeGaugeValueKind;
        const BUNDLE: &[GaugeDefinition] = &[
            GaugeDefinition {
                metric_kind: crate::proto::NativeMetricKind::Gauge,
                meter_name: "",
                groups: &[],
                name: "bundleSize",
                kind: NativeGaugeValueKind::Int32,
            },
            GaugeDefinition {
                metric_kind: crate::proto::NativeMetricKind::Gauge,
                meter_name: "",
                groups: &[],
                name: "bundleRatio",
                kind: NativeGaugeValueKind::Float64,
            },
        ];
        Ok(
            if self.0.lock().map_err(|_| poisoned())?.plan.mini_batch_size > 0 {
                BUNDLE
            } else {
                &[]
            },
        )
    }
    fn write_gauge_values(&self, values: &mut [i64]) -> Result<()> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        if processor.plan.mini_batch_size > 0 {
            // Match Flink's signed Integer count, including its wraparound semantics.
            let elements = processor.pending_elements as i32;
            let keys = processor.pending.len();
            let ratio = if keys == 0 {
                0.0
            } else {
                f64::from(elements) / keys as f64
            };
            values.copy_from_slice(&[i64::from(elements), ratio.to_bits() as i64]);
        }
        Ok(())
    }
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, event: ControlEvent) -> bool {
        matches!(
            event,
            ControlEvent::Watermark(_) | ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput
        ) && self.0.lock().is_ok_and(|processor| {
            processor.plan.mini_batch_size > 0 && !processor.plan.bounded_final_output
        })
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_native_node(node)?;
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "group aggregate requires one native child".into(),
            ));
        }
        Ok(Arc::new(
            UnaryExec::new(self.0.clone(), children.remove(0))?.with_node_id(node.plan_node_id),
        ))
    }
    fn snapshot(&self, group: u32) -> Result<crate::state::SnapshotBytes> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        processor.require_native_state_boundary()?;
        processor.snapshot_key_group(group)
    }
    fn restore(&self, group: u32, bytes: &[u8]) -> Result<()> {
        let mut processor = self.0.lock().map_err(|_| poisoned())?;
        processor.require_native_state_boundary()?;
        processor.restore_key_group(group, bytes)
    }
    fn restore_from_checkpoint(
        &self,
        group: u32,
        source: &crate::state::RocksPluginKeyedState,
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        let mut processor = self.0.lock().map_err(|_| poisoned())?;
        processor.require_native_state_boundary()?;
        processor.invocation.require_idle("group aggregate")?;
        let GroupAggregateProcessor {
            state,
            membership_layout,
            ..
        } = &mut *processor;
        crate::state::import_key_group(state.as_mut(), source, group, owner, &mut |_, value| {
            membership::validate_restored_header(membership_layout.as_ref(), value)
        })
    }

    fn checkpoint(&self, directory: &std::path::Path) -> Result<()> {
        let processor = self.0.lock().map_err(|_| poisoned())?;
        processor.require_native_state_boundary()?;
        processor.checkpoint(directory)
    }
}

/// Validate before opening any state resources. Flink control callbacks, not invocation EOF,
/// drive mini-batch flush in the common region. Bounded-final output remains unsupported here.
pub(crate) fn validate_native_node(node: &proto::Operator) -> Result<()> {
    match &node.operator {
        Some(proto::operator::Operator::GroupAggregate(plan)) if !plan.bounded_final_output => {
            validate_plan(plan, 1)
        }
        Some(proto::operator::Operator::GlobalGroupAggregate(plan))
            if !plan.bounded_final_output =>
        {
            if plan.mini_batch_size == 0 {
                return Err(DataFusionError::Plan(
                    "global aggregate mini-batch size must be positive".into(),
                ));
            }
            planning::validate_mini_batch(
                plan.mini_batch_size,
                plan.input_schema.as_ref(),
                plan.output_schema.as_ref(),
            )
        }
        Some(
            proto::operator::Operator::GroupAggregate(_)
            | proto::operator::Operator::GlobalGroupAggregate(_),
        ) => Err(DataFusionError::Plan(
            "shared group aggregate requires bounded-final control lifecycle migration".into(),
        )),
        _ => Err(DataFusionError::Plan(
            "group aggregate binding requires a GroupAggregate or GlobalGroupAggregate node".into(),
        )),
    }
}

impl UnaryBatchProcessor for GroupAggregateProcessor {
    const NAME: &'static str = "StreamFusionGroupAggregateExec";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        if self.plan.bounded_final_output {
            return Err(DataFusionError::Plan(
                "shared group aggregate bounded-final control migration is unfinished".into(),
            ));
        }
        let envelope = Envelope::from_schema(input.as_ref())?;
        if input.fields()[..envelope.payload_width]
            .iter()
            .any(|field| {
                matches!(
                    field.name().as_str(),
                    "__streamfusion_key"
                        | "__streamfusion_input_row_kind"
                        | "__streamfusion_row_kind"
                        | INPUT_ROW
                )
            })
        {
            return Err(DataFusionError::Plan(
                "group aggregate payload conflicts with native envelope metadata".into(),
            ));
        }
        if let Some(planned) = &self.plan.input_schema {
            let planned = crate::planner::arrow_schema(planned)?;
            if planned.fields().len() != envelope.payload_width
                || planned
                    .fields()
                    .iter()
                    .zip(&input.fields()[..envelope.payload_width])
                    .any(|(expected, actual)| expected.data_type() != actual.data_type())
            {
                return Err(DataFusionError::Plan(
                    "group aggregate native input differs from its planned SQL schema".into(),
                ));
            }
        }
        // Validate output types before preparing/retaining the input schema or changing state.
        if let Some(output) = &self.plan.output_schema {
            let output = crate::planner::arrow_schema(output)?;
            let types = self
                .plan
                .grouping_indices
                .iter()
                .map(|index| {
                    input
                        .fields()
                        .get(*index as usize)
                        .filter(|_| (*index as usize) < envelope.payload_width)
                        .map(|field| field.data_type())
                        .ok_or_else(|| {
                            DataFusionError::Plan(
                                "group aggregate key is outside the payload".into(),
                            )
                        })
                })
                .chain(self.calls.iter().map(|call| Ok(&call.output_type)))
                .collect::<Result<Vec<_>>>()?;
            if output.fields().len() != types.len()
                || output
                    .fields()
                    .iter()
                    .zip(types)
                    .any(|(field, data_type)| field.data_type() != data_type)
            {
                return Err(DataFusionError::Plan(
                    "group aggregate planned output types differ from its keys and calls".into(),
                ));
            }
        }
        self.prepare_schema(input.clone(), input.fields().len())?;
        if !self.native_envelope {
            let mut fields = self
                .output_schema
                .as_ref()
                .expect("prepared output")
                .fields()
                .to_vec();
            self.native_owned_envelope = self.plan.mini_batch_size > 0
                || input
                    .fields()
                    .iter()
                    .any(|field| field.name() == OWNED_TIMESTAMP_V1);
            if self.native_owned_envelope {
                fields.insert(
                    fields.len() - 1,
                    Arc::new(Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true)),
                );
            }
            fields.push(Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)));
            self.output_schema = Some(Arc::new(Schema::new(fields)));
            self.native_envelope = true;
        }
        Ok(self.output_schema.clone().expect("prepared output"))
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        self.prepare_schema(input.schema(), input.num_columns())?;
        crate::planner::operators::envelope::validate_owned_input(&input)?;
        // Reject invalid changelog bytes before any group is updated. Insert-only contracts
        // cannot silently reinterpret retractions delivered by an upstream native stage.
        if let Some(index) = self.input_kind_index {
            let kinds = input
                .column(index)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("group aggregate RowKind must be Int8".into())
                })?;
            if kinds.null_count() != 0
                || kinds.values().iter().any(|kind| {
                    !(INSERT..=DELETE).contains(kind)
                        || (!self.plan.input_changelog && *kind != INSERT)
                })
            {
                return Err(DataFusionError::Execution(
                    "group aggregate input violates its RowKind/changelog contract".into(),
                ));
            }
        }
        let base = input
            .num_rows()
            .saturating_mul(192usize.saturating_add(self.calls.len().saturating_mul(64)));
        self.scratch_reservation.resize(base)?;
        let result = (|| {
            let output = if input.num_rows() == 0 {
                // Control invocations enter through typed empty ports. Do not prefetch or
                // re-admit the entire pending bundle just to deliver an empty child batch.
                let schema = self
                    .output_schema
                    .clone()
                    .expect("native output schema prepared");
                self.scratch_reservation
                    .resize(schema.fields().len().saturating_mul(4096))?;
                RecordBatch::new_empty(schema)
            } else if self.partial_input {
                self.process_partial_mini_accounted(input, base)?
            } else if self.plan.mini_batch_size == 0 {
                self.process_arrow_accounted(input, base)?
            } else {
                self.process_raw_mini_batch(input, base)?
            };
            let memory = self.scratch_reservation.split(
                output.get_array_memory_size(),
                "group aggregate native output",
            )?;
            crate::memory_pool::arrow_lease::host_batch(output, memory)
        })();
        self.scratch_reservation.resize(0)?;
        result
    }

    fn poll_control(&mut self, _event: ControlEvent) -> Result<Option<RecordBatch>> {
        if self.plan.mini_batch_size == 0 || self.plan.bounded_final_output {
            return Err(DataFusionError::Plan(
                "group aggregate has no control binding for this mode".into(),
            ));
        }
        let result = self.drain_native_bundle();
        self.scratch_reservation.resize(0)?;
        result
    }
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("group aggregate native state lock is poisoned".into())
}

#[cfg(test)]
mod tests;
