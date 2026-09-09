// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use crate::planner::operators::envelope::{processing_time, Envelope};
use crate::planner::persistent::unary::{InvocationState, UnaryBatchProcessor, UnaryExec};
use crate::planner::persistent::PersistentOperatorFactory;
use crate::proto;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, Int8Array, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use prost::Message;
use std::sync::{Arc, Mutex};

struct Consumer {
    invocation: InvocationState,
    seen: Arc<Mutex<Vec<i64>>>,
}
impl UnaryBatchProcessor for Consumer {
    const NAME: &'static str = "ClockConsumerFixture";
    fn invocation(&mut self) -> &mut InvocationState {
        &mut self.invocation
    }
    fn prepare_output_schema(&mut self, input: SchemaRef) -> Result<SchemaRef> {
        let clock = Envelope::from_schema(input.as_ref())?.payload_width;
        Ok(Arc::new(
            input.project(
                &(0..input.fields().len())
                    .filter(|&i| i != clock)
                    .collect::<Vec<_>>(),
            )?,
        ))
    }
    fn process_batch(&mut self, input: RecordBatch) -> Result<RecordBatch> {
        self.seen
            .lock()
            .unwrap()
            .extend_from_slice(processing_time::column(&input)?.unwrap().values());
        let clock = Envelope::from_schema(input.schema().as_ref())?.payload_width;
        Ok(input.project(
            &(0..input.num_columns())
                .filter(|&i| i != clock)
                .collect::<Vec<_>>(),
        )?)
    }
}
struct Factory {
    kernel: Arc<Mutex<Consumer>>,
    slot: usize,
    supported: bool,
}
impl PersistentOperatorFactory for Factory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }
    fn supports_control(&self, event: ControlEvent) -> bool {
        self.supported && matches!(event, ControlEvent::ProcessingTime(_))
    }
    fn processing_time_input(&self) -> Option<usize> {
        Some(self.slot)
    }
    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            UnaryExec::new(self.kernel.clone(), children.remove(0))?
                .with_node_id(node.plan_node_id),
        ))
    }
}
fn context(region: bool, intervening: bool) -> (NativeExecutionContext, Arc<TestBroker>) {
    let broker = Arc::new(TestBroker::new(8 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 8 << 20));
    let input = proto::Operator {
        plan_node_id: if region { 0 } else { 1 },
        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
        ..Default::default()
    };
    let calc = proto::Operator {
        plan_node_id: 3,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(input.clone())),
            preserve_input_envelope: true,
            projections: vec![proto::Expression {
                expression: Some(proto::expression::Expression::InputReference(
                    proto::InputReference {
                        index: 0,
                        r#type: None,
                    },
                )),
            }],
            condition: None,
        }))),
        ..Default::default()
    };
    let owner = proto::Operator {
        plan_node_id: 2,
        operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
            proto::GroupAggregate {
                input: Some(Box::new(if intervening && !region {
                    calc.clone()
                } else {
                    input
                })),
                ..Default::default()
            },
        ))),
        ..Default::default()
    };
    let context = if region {
        use proto::native_region_input_reference::Source;
        let mut stages = Vec::new();
        if intervening {
            stages.push(proto::NativeRegionStage {
                operator: Some(calc),
                inputs: vec![proto::NativeRegionInputReference {
                    source: Some(Source::ExternalInput(0)),
                }],
            });
        }
        stages.push(proto::NativeRegionStage {
            operator: Some(owner),
            inputs: vec![proto::NativeRegionInputReference {
                source: Some(if intervening {
                    Source::StageId(3)
                } else {
                    Source::ExternalInput(0)
                }),
            }],
        });
        NativeExecutionContext::new_region(
            &proto::NativeRegionPlan {
                protocol_version: 1,
                input_count: 1,
                stages,
                output_stage_ids: vec![2],
            }
            .encode_to_vec(),
            pool,
        )
        .unwrap()
    } else {
        NativeExecutionContext::new(
            &proto::NativePlan {
                protocol_version: 3,
                root: Some(owner),
            }
            .encode_to_vec(),
            pool,
        )
        .unwrap()
    };
    (context, broker)
}
fn factory(
    slot: usize,
    supported: bool,
) -> (Arc<dyn PersistentOperatorFactory>, Arc<Mutex<Vec<i64>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::new(Factory {
            kernel: Arc::new(Mutex::new(Consumer {
                invocation: InvocationState::Idle,
                seen: seen.clone(),
            })),
            slot,
            supported,
        }),
        seen,
    )
}
fn input() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "value",
            Arc::new(Int32Array::from(vec![Some(7), None, Some(-3)])) as ArrayRef,
        ),
        (
            "__streamfusion_row_kind",
            Arc::new(Int8Array::from(vec![0, 1, 3])) as ArrayRef,
        ),
        (
            "__streamfusion_stream_record_timestamp",
            Arc::new(Int64Array::from(vec![None, Some(17), None])) as ArrayRef,
        ),
    ])
    .unwrap()
}
fn clock(values: Vec<Option<i64>>, nullable: bool) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            processing_time::FIELD,
            DataType::Int64,
            nullable,
        )])),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}
fn export(batch: &RecordBatch) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    arrow::ffi::to_ffi(&StructArray::from(batch.clone()).to_data()).unwrap()
}

#[test]
fn clock_inputs_cross_the_same_c_data_edge_and_are_consumed_in_tree_and_region() {
    for region in [false, true] {
        for decoded in [false, true] {
            let (mut context, broker) = context(region, false);
            let (factory, seen) = factory(0, true);
            context.bind_persistent(vec![(2, factory)]).unwrap();
            let context = Arc::new(context);
            let (caps, memory) = context.control_capabilities().unwrap();
            let caps = proto::NativeControlCapabilities::decode(caps.as_slice()).unwrap();
            assert_eq!(caps.protocol_version, 3);
            assert_eq!(caps.stages[0].processing_time_input_port, Some(0));
            drop(memory);
            let expected = [i64::MAX, i64::MIN, -1];
            let data = input();
            let clocks = clock(expected.into_iter().map(Some).collect(), false);
            let (mut array, mut schema) = export(&if decoded {
                data.slice(0, 0)
            } else {
                data.clone()
            });
            let (mut clock_array, mut clock_schema) = export(&clocks);
            let replacement = decoded.then(|| {
                (
                    0,
                    super::super::common::prepare_input(&context, data.clone(), 0).unwrap(),
                )
            });
            let (batches, reservations) = unsafe {
                import_inputs(
                    &context,
                    &[
                        &mut array as *mut _ as jlong,
                        &mut clock_array as *mut _ as jlong,
                    ],
                    &[
                        &mut schema as *mut _ as jlong,
                        &mut clock_schema as *mut _ as jlong,
                    ],
                    context.reservation("fixture"),
                    replacement,
                )
            }
            .unwrap();
            assert!(array.is_released() && clock_array.is_released());
            assert!(schema.release.is_none() && clock_schema.release.is_none());
            assert_eq!(
                batches[0].column(0).to_data().buffers()[0].as_ptr(),
                data.column(0).to_data().buffers()[0].as_ptr()
            );
            assert_eq!(
                processing_time::column(&batches[0])
                    .unwrap()
                    .unwrap()
                    .values()
                    .as_ptr(),
                clocks
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ptr()
            );
            let output = context.runtime().block_on(async {
                let mut output = Vec::new();
                if region {
                    let mut stream = context.start_region(batches).unwrap();
                    while let Some(batch) = stream.next().await {
                        output.push(batch.unwrap().batch);
                    }
                } else {
                    let mut stream = context.start(batches).unwrap();
                    while let Some(batch) = stream.next().await {
                        output.push(batch.unwrap());
                    }
                }
                output
            });
            assert_eq!(*seen.lock().unwrap(), expected);
            assert_eq!(
                output.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                3
            );
            for batch in &output {
                assert!(processing_time::column(batch).unwrap().is_none());
            }
            drop(output);
            drop(reservations);
            drop(context);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn clock_binding_rejects_intervening_stages_invalid_slots_and_missing_timer_capability() {
    for region in [false, true] {
        for (intervening, slot, supported) in [(true, 0, true), (false, 1, true), (false, 0, false)]
        {
            let (mut context, broker) = context(region, intervening);
            let (factory, _) = factory(slot, supported);
            let before = broker.reserved();
            assert!(context.bind_persistent(vec![(2, factory)]).is_err());
            assert!(context.clock_input_bindings().is_empty());
            context.require_idle().unwrap();
            assert_eq!(broker.reserved(), before);
        }
    }
}

#[test]
fn clock_import_rejects_missing_mismatched_and_nullable_vectors_before_invocation() {
    for invalid in [
        clock(vec![Some(1)], false),
        clock(vec![Some(1), None, Some(3)], true),
    ] {
        let (mut context, broker) = context(false, false);
        let (factory, seen) = factory(0, true);
        context.bind_persistent(vec![(2, factory)]).unwrap();
        let context = Arc::new(context);
        let (mut array, mut schema) = export(&input());
        let (mut clock_array, mut clock_schema) = export(&invalid);
        assert!(unsafe {
            import_inputs(
                &context,
                &[
                    &mut array as *mut _ as jlong,
                    &mut clock_array as *mut _ as jlong,
                ],
                &[
                    &mut schema as *mut _ as jlong,
                    &mut clock_schema as *mut _ as jlong,
                ],
                context.reservation("fixture"),
                None,
            )
        }
        .is_err());
        context.require_idle().unwrap();
        assert!(seen.lock().unwrap().is_empty());
        let unclocked = super::super::common::prepare_input(&context, input(), 0).unwrap();
        assert!(context.start(vec![unclocked]).is_err());
        context.require_idle().unwrap();
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}
