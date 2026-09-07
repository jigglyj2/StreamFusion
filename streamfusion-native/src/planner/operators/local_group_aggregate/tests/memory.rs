// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::MemoryReservationBroker;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    peak: AtomicUsize,
    transferred: AtomicUsize,
    deny_transfer: AtomicBool,
}
impl Broker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: TestBroker::new(64 << 20),
            peak: AtomicUsize::new(0),
            transferred: AtomicUsize::new(0),
            deny_transfer: AtomicBool::new(false),
        })
    }
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let accepted = self.inner.try_reserve(bytes)?;
        if accepted {
            self.peak.fetch_max(
                self.inner.reserved() + self.transferred.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        }
        Ok(accepted)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
    fn transfer_to_arrow(&self, bytes: usize) -> Result<()> {
        if self.deny_transfer.load(Ordering::Relaxed) {
            return Err(DataFusionError::Execution(
                "test rejected Arrow transfer".into(),
            ));
        }
        self.transferred.fetch_add(bytes, Ordering::Relaxed);
        self.inner.release(bytes)
    }
}

#[test]
fn nested_constructor_decode_and_codec_allocations_are_admitted() {
    for width in [1, 64, 512] {
        let mut wire = proto::NativePlan::decode(plan(100, true).as_slice()).unwrap();
        let proto::operator::Operator::LocalGroupAggregate(local) =
            wire.root.as_mut().unwrap().operator.as_mut().unwrap()
        else {
            unreachable!()
        };
        let key = proto::LogicalType {
            nullable: true,
            r#type: Some(proto::logical_type::Type::Row(proto::RowType {
                fields: (0..width)
                    .map(|index| proto::RowField {
                        name: format!("child-{index}"),
                        r#type: Some(logical_bigint(true)),
                    })
                    .collect(),
            })),
        };
        local.input_schema.as_mut().unwrap().fields[0].r#type = Some(key.clone());
        local.output_schema.as_mut().unwrap().fields[0].r#type = Some(key);
        let (_, sizing) = measure(|| {
            super::super::super::group_aggregate::schema_admission::planned_schemas(
                local.input_schema.as_ref(),
                local.output_schema.as_ref(),
                &local.aggregate_calls,
            )
            .unwrap()
        });
        assert_eq!(sizing.peak, 0);
        let bytes = wire.encode_to_vec();
        let (_, scanned) =
            measure(|| crate::execution_context::wire_memory::PlanMemory::scan(&bytes).unwrap());
        assert_eq!(scanned.peak, 0);
        let broker = Broker::new();
        let memory = HostMemoryReservation::new(broker.clone(), "local constructor allocation");
        let (processor, observed) =
            measure(|| LocalGroupAggregateProcessor::new(&bytes, memory).unwrap());
        assert!(
            observed.peak <= broker.peak.load(Ordering::Relaxed),
            "width={width} {observed:?}"
        );
        assert!(
            observed.live as usize <= broker.inner.reserved(),
            "width={width} {observed:?} reserved={}",
            broker.inner.reserved()
        );
        drop(processor);
        assert_eq!(broker.inner.reserved(), 0);

        let decoded = crate::execution_context::wire_memory::PlanMemory::scan(&bytes)
            .unwrap()
            .decoded()
            .unwrap();
        let limited = Arc::new(TestBroker::new(decoded + 1024));
        let error = LocalGroupAggregateProcessor::new(
            &bytes,
            HostMemoryReservation::new(limited.clone(), "denied local schema"),
        );
        assert!(error
            .err()
            .unwrap()
            .to_string()
            .contains("planned schemas and codecs"));
        assert_eq!(limited.reserved(), 0);
    }
}

#[test]
fn batch_and_flush_peak_and_live_heap_fit_admission_including_retained_output() {
    use arrow::array::StringArray;
    for strings in [false, true] {
        for trigger in [1, 7, 100_000] {
            let mut wire = proto::NativePlan::decode(plan(trigger, true).as_slice()).unwrap();
            let input = if strings {
                let proto::operator::Operator::LocalGroupAggregate(local) =
                    wire.root.as_mut().unwrap().operator.as_mut().unwrap()
                else {
                    unreachable!()
                };
                let varchar = proto::LogicalType {
                    nullable: true,
                    r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
                };
                local.input_schema.as_mut().unwrap().fields[0].r#type = Some(varchar.clone());
                local.input_schema.as_mut().unwrap().fields[1].r#type = Some(varchar.clone());
                local.output_schema.as_mut().unwrap().fields[0].r#type = Some(varchar.clone());
                local.aggregate_calls[1].function = proto::AggregateFunction::Min as i32;
                local.aggregate_calls[1].input_type = Some(varchar.clone());
                local.aggregate_calls[1].output_type = Some(varchar);
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("key", DataType::Utf8, true),
                        Field::new("value", DataType::Utf8, true),
                        Field::new("__streamfusion_input_row_kind", DataType::Int8, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from_iter(
                            (0..400).map(|row| (row % 9 != 0).then(|| format!("é-{row}"))),
                        )),
                        Arc::new(StringArray::from_iter((0..400).map(|row| {
                            (row % 4 != 0).then(|| format!("{row:05}-{}", "界".repeat(128)))
                        }))),
                        Arc::new(Int8Array::from(vec![0; 400])),
                    ],
                )
                .unwrap()
            } else {
                batch((0..400).collect(), (0..400).collect(), Some(vec![0; 400]))
            };
            let bytes = wire.encode_to_vec();
            let broker = Broker::new();
            let memory = HostMemoryReservation::new(broker.clone(), "local batch allocation");
            let ((processor, outputs), observed) = measure(|| {
                let mut processor = LocalGroupAggregateProcessor::new(&bytes, memory).unwrap();
                let mut outputs = Vec::new();
                for _ in 0..2 {
                    outputs.push(processor.process_arrow(input.clone()).unwrap());
                }
                outputs.push(processor.finish_bundle().unwrap());
                (processor, outputs)
            });
            assert!(
                observed.peak <= broker.peak.load(Ordering::Relaxed),
                "strings={strings} trigger={trigger} {observed:?} peak_credit={}",
                broker.peak.load(Ordering::Relaxed)
            );
            assert!(
                observed.live as usize
                    <= broker.inner.reserved() + broker.transferred.load(Ordering::Relaxed),
                "strings={strings} trigger={trigger} {observed:?} live_credit={} transferred={}",
                broker.inner.reserved(),
                broker.transferred.load(Ordering::Relaxed)
            );
            assert_eq!(
                processor.pending_reservation.size(),
                processor.pending_bytes()
            );
            assert!(processor.pending_reservation.size() > 0);
            drop(outputs);
            drop(processor);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn denied_workspace_and_invalid_rowkinds_leave_the_prior_bundle_unchanged() {
    let broker = Broker::new();
    let memory = HostMemoryReservation::new(broker.clone(), "local retryable admission");
    let mut blocker = memory.sibling("test occupied memory");
    let mut processor = LocalGroupAggregateProcessor::new(&plan(100, true), memory).unwrap();
    processor
        .process_arrow(batch(vec![1], vec![10], Some(vec![0])))
        .unwrap();
    let before = processor.pending_bytes();
    let before_reserved = broker.inner.reserved();
    assert!(processor
        .process_arrow(batch(vec![1, 2], vec![20, 30], Some(vec![0, 99])))
        .is_err());
    assert_eq!(processor.pending_element_count(), 1);
    assert_eq!(processor.pending_bytes(), before);
    assert_eq!(broker.inner.reserved(), before_reserved);
    blocker
        .resize(broker.available().unwrap().unwrap() - 1024)
        .unwrap();
    assert!(processor
        .process_arrow(batch(vec![1, 2], vec![20, 30], Some(vec![0, 0])))
        .unwrap_err()
        .to_string()
        .contains("batch workspace"));
    assert!(processor
        .finish_bundle()
        .unwrap_err()
        .to_string()
        .contains("batch workspace"));
    assert_eq!(processor.pending_element_count(), 1);
    assert_eq!(processor.pending_bytes(), before);
    blocker.resize(0).unwrap();
    let output = processor.finish_bundle().unwrap();
    let bytes = output
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(decode_state(bytes, &processor.calls).unwrap().row_count, 1);
    drop(output);
    drop(processor);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn failed_output_transfer_drops_mutated_storage_and_requires_recovery() {
    let broker = Broker::new();
    let memory = HostMemoryReservation::new(broker.clone(), "local failed edge");
    let mut processor = LocalGroupAggregateProcessor::new(&plan(7, true), memory).unwrap();
    broker.deny_transfer.store(true, Ordering::Relaxed);
    assert!(processor
        .process_arrow(batch(vec![1], vec![10], Some(vec![0])))
        .unwrap_err()
        .to_string()
        .contains("rejected Arrow transfer"));
    assert_eq!(processor.pending_bytes(), 0);
    assert_eq!(processor.pending_reservation.size(), 0);
    assert_eq!(processor.workspace.size(), 0);
    assert!(processor
        .finish_bundle()
        .unwrap_err()
        .to_string()
        .contains("requires recovery"));
    drop(processor);
    assert_eq!(broker.inner.reserved(), 0);
}
