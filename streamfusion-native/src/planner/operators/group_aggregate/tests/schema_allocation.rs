// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::MemoryReservationBroker;

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

fn nested_plan(width: usize, depth: usize, nested_count: bool) -> Vec<u8> {
    let mut plan = proto::NativePlan::decode(plan(true, true).as_slice()).unwrap();
    let Some(proto::operator::Operator::GroupAggregate(group)) =
        plan.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    let mut key = proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Row(proto::RowType {
            fields: (0..width)
                .map(|index| proto::RowField {
                    name: format!("f{index}"),
                    r#type: Some(logical_varchar(true)),
                })
                .collect(),
        })),
    };
    for _ in 0..depth {
        key = proto::LogicalType {
            nullable: true,
            r#type: Some(proto::logical_type::Type::Row(proto::RowType {
                fields: vec![proto::RowField {
                    name: "wrapped".into(),
                    r#type: Some(proto::LogicalType {
                        nullable: true,
                        r#type: Some(proto::logical_type::Type::Array(Box::new(
                            proto::CollectionType {
                                element_type: Some(Box::new(key)),
                            },
                        ))),
                    }),
                }],
            })),
        };
    }
    if nested_count {
        group.aggregate_calls[0].function = proto::AggregateFunction::Count as i32;
        group.aggregate_calls[0].input_index = Some(0);
        group.aggregate_calls[0].input_type = Some(key.clone());
    }
    let mut input = group_schema();
    input.fields[0].r#type = Some(key.clone());
    let mut output = group_output_schema();
    output.fields[0].r#type = Some(key);
    group.input_schema = Some(input);
    group.output_schema = Some(output);
    group.mini_batch_size = 100;
    plan.encode_to_vec()
}

#[test]
fn retained_nested_schema_and_codec_heap_fits_constructor_credit() {
    for width in [1, 64, 512] {
        for depth in [0, 3] {
            for nested_count in [false, true] {
                let serialized = nested_plan(width, depth, nested_count);
                // Verify pre-decode admission separately: a later schema reservation must not
                // conceal an allocation made before that credit exists.
                let (decoded, decode_heap) = measure(|| decode_plan(&serialized).unwrap());
                assert!(
                    decode_heap.peak <= serialized.len() * 8 + 4096,
                    "decode width={width} depth={depth} heap={decode_heap:?}"
                );
                let Some(proto::operator::Operator::GroupAggregate(group)) =
                    decoded.root.as_ref().unwrap().operator.as_ref()
                else {
                    unreachable!()
                };
                let (_, sizing_heap) =
                    measure(|| schema_admission::planned_workspace(group).unwrap());
                assert_eq!(sizing_heap.peak, 0);
                drop(decoded);
                let broker = Arc::new(PeakBroker {
                    inner: TestBroker::new(64 << 20),
                    peak: AtomicUsize::new(0),
                });
                let reservation =
                    HostMemoryReservation::new(broker.clone(), "schema allocation test");
                let (processor, observed) = measure(|| {
                    GroupAggregateProcessor::new(&serialized, 128, 0, 127, reservation).unwrap()
                });
                assert!(observed.live >= 0);
                assert!(
                    observed.peak <= broker.peak.load(Ordering::Relaxed),
                    "constructor width={width} depth={depth} heap={observed:?} admitted={}",
                    broker.peak.load(Ordering::Relaxed)
                );
                assert!(
                    observed.live as usize <= broker.inner.reserved(),
                    "width={width} serialized={} observed={observed:?} admitted={}",
                    serialized.len(),
                    broker.inner.reserved()
                );
                assert!(
                    broker.inner.reserved() < broker.peak.load(Ordering::Relaxed),
                    "temporary schema credit must be released"
                );
                drop(processor);
                assert_eq!(broker.inner.reserved(), 0);
            }
        }
    }
}

#[test]
fn denied_planned_schema_credit_releases_constructor_owners() {
    let serialized = nested_plan(512, 0, false);
    // Enough for state + admitted protobuf decode, but not planned Arrow codecs.
    let decoded = crate::execution_context::wire_memory::PlanMemory::scan(&serialized)
        .unwrap()
        .decoded()
        .unwrap();
    let broker = Arc::new(TestBroker::new(decoded + 4096 + 8192));
    let result = GroupAggregateProcessor::new(
        &serialized,
        128,
        0,
        127,
        HostMemoryReservation::new(broker.clone(), "denied schema test"),
    );
    let error = match result {
        Ok(_) => panic!("schema construction must fail admission"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("planned Arrow schemas and codecs"),
        "{error}"
    );
    assert_eq!(broker.reserved(), 0);
}
