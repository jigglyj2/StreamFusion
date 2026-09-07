// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, MemoryReservationBroker};
use arrow::array::{new_null_array, RecordBatch};
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
struct PeakBroker {
    inner: TestBroker,
    peak: AtomicUsize,
}
impl MemoryReservationBroker for PeakBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let accepted = self.inner.try_reserve(bytes)?;
        if accepted {
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        }
        Ok(accepted)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}

pub(super) fn wide_plan(width: usize, nested: bool, stages: usize) -> (Vec<u8>, RecordBatch) {
    let scalar = proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
    };
    let fields = (0..width)
        .map(|i| proto::Field {
            name: format!("f{i}"),
            r#type: Some(scalar.clone()),
        })
        .collect::<Vec<_>>();
    let schema = proto::Schema {
        fields: if nested {
            vec![proto::Field {
                name: "nested".into(),
                r#type: Some(proto::LogicalType {
                    nullable: true,
                    r#type: Some(proto::logical_type::Type::Row(proto::RowType {
                        fields: fields
                            .into_iter()
                            .map(|f| proto::RowField {
                                name: f.name,
                                r#type: f.r#type,
                            })
                            .collect(),
                    })),
                }),
            }]
        } else {
            fields
        },
    };
    let arrow = crate::planner::arrow_schema(&schema).unwrap();
    let input = RecordBatch::try_new(
        arrow.clone(),
        arrow
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), 1))
            .collect(),
    )
    .unwrap();
    let count = schema.fields.len();
    let mut plan = proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 1,
            metric_name: "wide Calc".into(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                input: Some(Box::new(proto::Operator {
                    plan_node_id: 2,
                    metric_name: "input".into(),
                    clear_record_timestamps: false,
                    metric_uid: None,
                    operator: Some(proto::operator::Operator::Input(proto::Input {
                        schema: Some(schema),
                        input_index: 0,
                    })),
                })),
                projections: (0..count)
                    .map(|i| proto::Expression {
                        expression: Some(proto::expression::Expression::InputReference(
                            proto::InputReference {
                                index: i as u32,
                                r#type: None,
                            },
                        )),
                    })
                    .collect(),
                condition: None,
                preserve_input_envelope: false,
            }))),
        }),
    };
    for stage in 1..stages {
        let child = plan.root.take().unwrap();
        let Some(proto::operator::Operator::Calc(calc)) = &child.operator else {
            unreachable!()
        };
        let projections = calc.projections.clone();
        plan.root = Some(proto::Operator {
            plan_node_id: (stage + 2) as u64,
            metric_name: format!("Calc {stage}"),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                input: Some(Box::new(child)),
                projections,
                condition: None,
                preserve_input_envelope: false,
            }))),
        });
    }
    (plan.encode_to_vec(), input)
}

#[test]
fn common_context_and_cached_tree_cover_wide_schema_heap() {
    for width in [1, 64, 512, 4096] {
        for nested in [false, true] {
            for stages in [1, 3, 8] {
                let (bytes, input) = wide_plan(width, nested, stages);
                let broker = Arc::new(PeakBroker {
                    inner: TestBroker::new(128 << 20),
                    peak: AtomicUsize::new(0),
                });
                let pool: Arc<dyn MemoryPool> =
                    Arc::new(FlinkMemoryPool::new(broker.clone(), 128 << 20));
                // Source buffers are borrowed and remain alive outside the measured native scope.
                let (context, created) =
                    measure(|| NativeExecutionContext::new(&bytes, pool).unwrap());
                assert!(created.peak <= broker.peak.load(Ordering::Relaxed), "constructor width={width} nested={nested} stages={stages} heap={created:?} credit={}", broker.peak.load(Ordering::Relaxed));
                let (physical, observed) = measure(|| {
                    context.remember_input_schema(0, input.schema()).unwrap();
                    context.execute_plan(vec![input.clone()], Ok).unwrap()
                });
                assert!(observed.live >= 0);
                assert!(
                    (created.live + observed.live) as usize <= broker.inner.reserved(),
                    "width={width} nested={nested} observed={observed:?} retained={}",
                    broker.inner.reserved()
                );
                assert!(
                    created.live as usize + observed.peak <= broker.peak.load(Ordering::Relaxed),
                    "width={width} nested={nested} observed={observed:?} peak_credit={}",
                    broker.peak.load(Ordering::Relaxed)
                );
                assert!(
                    broker.inner.reserved() < broker.peak.load(Ordering::Relaxed),
                    "lowering workspace must not be retained"
                );
                let again = context.execute_plan(vec![input.clone()], Ok).unwrap();
                assert!(Arc::ptr_eq(&physical, &again));
                drop(again);
                drop(physical);
                drop(context);
                assert_eq!(broker.inner.reserved(), 0);
            }
        }
    }
}

#[test]
fn denied_common_lowering_workspace_leaves_no_cached_tree_and_can_retry() {
    let (bytes, input) = wide_plan(64, false, 3);
    let budget = 16 << 20;
    let broker = Arc::new(TestBroker::new(budget));
    let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), budget));
    let context = NativeExecutionContext::new(&bytes, pool).unwrap();
    let retained = context.physical_control_bytes
        + PHYSICAL_PLAN_BASE_BYTES
        + CACHED_SCHEMA_BASE_BYTES
        + input
            .schema()
            .fields()
            .iter()
            .map(|f| f.size())
            .sum::<usize>()
            * 2;
    let pressure = context.reservation("workspace pressure");
    pressure
        .try_grow(budget - broker.reserved() - retained)
        .unwrap();
    let before = broker.reserved();
    for _ in 0..3 {
        let error = match context.execute_plan(vec![input.clone()], Ok) {
            Ok(_) => panic!("workspace admission must fail"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("construction workspace"),
            "{error}"
        );
        assert!(context.physical_plan.lock().unwrap().is_none());
        assert_eq!(broker.reserved(), before);
    }
    drop(pressure);
    drop(context.execute_plan(vec![input], Ok).unwrap());
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
