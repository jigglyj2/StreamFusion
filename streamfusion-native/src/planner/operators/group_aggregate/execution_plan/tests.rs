// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::{tests_support::TestBroker, MemoryReservationBroker};
use futures::StreamExt;
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};

mod control;
mod partials;

fn bigint(nullable: bool) -> proto::LogicalType {
    proto::LogicalType {
        nullable,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
    }
}
fn calc(id: u64, child: proto::Operator, width: u32) -> proto::Operator {
    proto::Operator {
        plan_node_id: id,
        operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
            input: Some(Box::new(child)),
            preserve_input_envelope: true,
            condition: None,
            projections: (0..width)
                .map(|index| proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index,
                            r#type: None,
                        },
                    )),
                })
                .collect(),
        }))),
        ..Default::default()
    }
}
fn plan(changelog: bool) -> proto::NativePlan {
    let input = proto::Operator {
        plan_node_id: 1,
        operator: Some(proto::operator::Operator::Input(proto::Input::default())),
        ..Default::default()
    };
    let aggregate = proto::GroupAggregate {
        input: Some(Box::new(calc(2, input, 2))),
        grouping_indices: vec![0],
        input_changelog: changelog,
        generate_update_before: true,
        aggregate_calls: vec![
            proto::AggregateCall {
                function: proto::AggregateFunction::CountStar as i32,
                output_type: Some(bigint(false)),
                retractable: changelog,
                ..Default::default()
            },
            proto::AggregateCall {
                function: proto::AggregateFunction::Sum as i32,
                input_index: Some(1),
                input_type: Some(bigint(true)),
                output_type: Some(bigint(true)),
                retractable: changelog,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    proto::NativePlan {
        protocol_version: 2,
        root: Some(calc(
            4,
            proto::Operator {
                plan_node_id: 3,
                operator: Some(proto::operator::Operator::GroupAggregate(Box::new(
                    aggregate,
                ))),
                ..Default::default()
            },
            3,
        )),
    }
}
fn binding(rocks: Option<(&str, &std::path::Path)>) -> proto::NativeStateBindings {
    proto::NativeStateBindings {
        protocol_version: 1,
        bindings: vec![proto::NativeStateBinding {
            restored_watermark: None,
            plan_node_id: 3,
            max_parallelism: 16,
            first_key_group: 0,
            last_key_group: 15,
            backend: Some(match rocks {
                None => proto::native_state_binding::Backend::Memory(proto::NativeMemoryState {}),
                Some((plugin, directory)) => {
                    proto::native_state_binding::Backend::Rocksdb(proto::NativeRocksDbState {
                        log_directory: None,
                        plugin_path: plugin.into(),
                        database_path: directory.to_str().unwrap().into(),
                        memory_limit: 1 << 20,
                    })
                }
            }),
        }],
    }
}
fn batch(kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7, 7, 9])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(vec![11, 5, 30])),
        ],
    )
    .unwrap()
}
#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    transfers: AtomicUsize,
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.inner.try_reserve(bytes)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
    fn transfer_to_arrow(&self, bytes: usize) -> Result<()> {
        self.transfers.fetch_add(1, Ordering::Relaxed);
        self.inner.release(bytes)
    }
}
fn context(
    plan: &proto::NativePlan,
    binding: &proto::NativeStateBindings,
    memory: &HostMemoryReservation,
) -> Arc<NativeExecutionContext> {
    let mut context =
        NativeExecutionContext::new(&plan.encode_to_vec(), memory.datafusion_pool(32 << 20))
            .unwrap();
    context
        .install_state(&binding.encode_to_vec(), memory.sibling("bindings"))
        .unwrap();
    Arc::new(context)
}
fn run(context: &Arc<NativeExecutionContext>, input: RecordBatch) -> RecordBatch {
    let mut stream = context.start(vec![input]).unwrap();
    assert!(context.snapshot_state(3, 0).is_err());
    let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
    assert!(context.runtime().block_on(stream.next()).is_none());
    output
}

#[test]
fn aggregate_composes_between_calcs_and_restores_cross_backend_with_retractions() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks_first in [false, true] {
        if rocks_first && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(Broker {
            inner: TestBroker::new(32 << 20),
            transfers: AtomicUsize::new(0),
        });
        let memory = HostMemoryReservation::new(broker.clone(), "region");
        let mut snapshots: Vec<Vec<u8>> = Vec::new();
        for phase in 0..2 {
            let path = directory.path().join(format!("phase-{phase}"));
            let rocks = (rocks_first != (phase != 0))
                .then(|| plugin.as_ref().map(|p| (p.as_str(), path.as_path())))
                .flatten();
            let context = context(&plan(true), &binding(rocks), &memory);
            if phase != 0 {
                for (group, bytes) in snapshots.iter().enumerate() {
                    context.restore_state(3, group as u32, bytes).unwrap();
                }
            }
            let output = run(
                &context,
                batch(vec![if phase == 0 { INSERT } else { DELETE }; 3]),
            );
            assert_eq!(output.num_rows(), 4);
            let kinds = output
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values();
            assert_eq!(
                kinds.as_ref(),
                if phase == 0 {
                    &[0, 1, 2, 0]
                } else {
                    &[1, 2, 3, 3]
                }
            );
            let ordinals = output
                .column(4)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values();
            assert_eq!(
                ordinals.as_ref(),
                if phase == 0 {
                    &[11, 5, 5, 30]
                } else {
                    &[11, 11, 5, 30]
                }
            );
            assert_eq!(
                context.metric_snapshot().unwrap(),
                vec![4, 4, 4, 3, 3, 4, 2, 3, 3, 1, 0, 3]
            );
            if phase == 0 {
                snapshots = (0..16)
                    .map(|g| context.snapshot_state(3, g).unwrap().to_vec())
                    .collect();
            }
            let retained = output.column(1).slice(0, 1);
            drop(output);
            drop(context);
            assert!(
                broker.inner.reserved() > 0,
                "retained aggregate output stays admitted"
            );
            drop(retained);
            assert_eq!(
                broker.transfers.load(Ordering::Relaxed),
                0,
                "no Java transfer inside native plan"
            );
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn invalid_changelog_and_cancelled_aggregate_invocations_require_recovery() {
    for invalid in [true, false] {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "aggregate failures");
        let context = context(&plan(false), &binding(None), &memory);
        let mut stream = context
            .start(vec![batch(if invalid {
                vec![0, 3, 0]
            } else {
                vec![0; 3]
            })])
            .unwrap();
        let output = context.runtime().block_on(stream.next()).unwrap();
        assert_eq!(output.is_err(), invalid);
        drop(output);
        drop(stream);
        assert!(context.snapshot_state(3, 0).is_err());
        assert!(context.start(vec![batch(vec![0; 3])]).is_err());
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn bounded_and_incomplete_mini_plans_are_rejected_before_opening_rocksdb() {
    let directory = tempfile::tempdir().unwrap();
    for bounded in [false, true] {
        let mut plan = plan(true);
        let proto::operator::Operator::Calc(calc) =
            plan.root.as_mut().unwrap().operator.as_mut().unwrap()
        else {
            unreachable!()
        };
        let proto::operator::Operator::GroupAggregate(aggregate) =
            calc.input.as_mut().unwrap().operator.as_mut().unwrap()
        else {
            unreachable!()
        };
        if bounded {
            aggregate.bounded_final_output = true;
        } else {
            aggregate.mini_batch_size = 4;
        }
        let broker = Arc::new(TestBroker::new(32 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "control rejection");
        let mut context =
            NativeExecutionContext::new(&plan.encode_to_vec(), memory.datafusion_pool(32 << 20))
                .unwrap();
        let path = directory.path().join(format!("unopened-{bounded}"));
        let error = context
            .install_state(
                &binding(Some(("/missing-plugin.so", &path))).encode_to_vec(),
                memory.sibling("binding"),
            )
            .unwrap_err();
        assert!(error.to_string().contains(if bounded {
            "control lifecycle migration"
        } else {
            "requires input and output schemas"
        }));
        assert!(!path.exists());
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn invalid_input_schema_does_not_retain_a_partial_key_codec_or_its_reservation() {
    let mut plan = plan(true);
    let proto::operator::Operator::Calc(root) = plan.root.take().unwrap().operator.unwrap() else {
        unreachable!()
    };
    plan.root = root.input.map(|input| *input);
    let broker = Arc::new(TestBroker::new(32 << 20));
    let mut processor = GroupAggregateProcessor::new(
        &plan.encode_to_vec(),
        16,
        0,
        15,
        HostMemoryReservation::new(broker.clone(), "schema retry"),
    )
    .unwrap();
    let before = broker.reserved();
    let invalid = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("__streamfusion_row_kind", DataType::Int8, false),
        Field::new(INPUT_ROW, DataType::Int32, false),
    ]));
    assert!(processor.prepare_output_schema(invalid).is_err());
    assert!(processor.grouping_converter.is_none());
    assert!(processor.input_schema.is_none());
    assert!(processor.output_schema.is_none());
    assert_eq!(broker.reserved(), before);
    processor
        .prepare_output_schema(batch(vec![0; 3]).schema())
        .unwrap();
    let output = processor.process_batch(batch(vec![0; 3])).unwrap();
    assert_eq!(output.num_rows(), 4);
    drop(output);
    drop(processor);
    assert_eq!(broker.reserved(), 0);
}
