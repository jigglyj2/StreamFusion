// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

fn schema(width: usize) -> proto::Schema {
    proto::Schema {
        fields: (0..width)
            .map(|index| proto::Field {
                name: format!("c{index}"),
                r#type: Some(bigint(index != 0)),
            })
            .collect(),
    }
}

fn mini_plan(two_stages: bool, upper_mini: bool) -> proto::NativePlan {
    let mut plan = plan(true);
    let proto::operator::Operator::Calc(root) =
        plan.root.as_mut().unwrap().operator.as_mut().unwrap()
    else {
        unreachable!()
    };
    let proto::operator::Operator::GroupAggregate(group) =
        root.input.as_mut().unwrap().operator.as_mut().unwrap()
    else {
        unreachable!()
    };
    group.mini_batch_size = 100_000;
    group.input_schema = Some(schema(2));
    let mut output = schema(3);
    output.fields[1].r#type = Some(bigint(false));
    group.output_schema = Some(output.clone());
    if two_stages {
        let mut upper = group.clone();
        upper.input_schema = Some(output);
        upper.mini_batch_size = if upper_mini { 100_000 } else { 0 };
        upper.input = plan.root.take().map(Box::new);
        plan.root = Some(calc(
            6,
            proto::Operator {
                plan_node_id: 5,
                operator: Some(proto::operator::Operator::GroupAggregate(upper)),
                ..Default::default()
            },
            3,
        ));
    }
    plan
}

fn input(rows: usize, kind: i8) -> RecordBatch {
    let schema = batch(vec![INSERT; 3]).schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(Int8Array::from(vec![kind; rows])),
            Arc::new(Int32Array::from_iter_values(0..rows as i32)),
        ],
    )
    .unwrap()
}

pub(super) fn drain(
    context: &Arc<NativeExecutionContext>,
    input: RecordBatch,
    events: &[(u64, ControlEvent)],
) -> Vec<RecordBatch> {
    let mut stream = context.start_control(vec![input], events).unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        batches.push(batch.unwrap());
    }
    batches
}

#[test]
fn real_bundles_drain_through_calcs_and_adjacent_aggregates_on_both_backends() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        for upper_mini in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let broker = Arc::new(Broker {
                inner: TestBroker::new(128 << 20),
                transfers: AtomicUsize::new(0),
            });
            let memory = HostMemoryReservation::new(broker.clone(), "real control tree");
            let lower_path = directory.path().join("lower");
            let upper_path = directory.path().join("upper");
            let mut bindings =
                binding(rocks.then(|| (plugin.as_ref().unwrap().as_str(), lower_path.as_path())));
            let mut upper_binding =
                binding(rocks.then(|| (plugin.as_ref().unwrap().as_str(), upper_path.as_path())))
                    .bindings
                    .remove(0);
            upper_binding.plan_node_id = 5;
            bindings.bindings.push(upper_binding);
            let context = context(&mini_plan(true, upper_mini), &bindings, &memory);
            let baseline = broker.inner.reserved();
            let (wire, schema_credit) = context.gauge_schema().unwrap();
            let schema = proto::NativeGaugeSchema::decode(wire.as_slice()).unwrap();
            assert_eq!(schema.protocol_version, 1);
            assert_eq!(schema.gauges.len(), if upper_mini { 4 } else { 2 });
            for pair in schema.gauges.chunks_exact(2) {
                assert_eq!(pair[0].name, "bundleSize");
                assert_eq!(
                    pair[0].value_kind,
                    proto::NativeGaugeValueKind::Int32 as i32
                );
                assert_eq!(pair[1].name, "bundleRatio");
                assert_eq!(
                    pair[1].value_kind,
                    proto::NativeGaugeValueKind::Float64 as i32
                );
                assert_eq!(pair[0].plan_node_id, pair[1].plan_node_id);
                assert!(pair.iter().all(|gauge| gauge.groups.is_empty()));
            }
            drop(wire);
            drop(schema_credit);
            assert_eq!(broker.inner.reserved(), baseline);
            let batch = input(5000, INSERT);
            assert_eq!(run(&context, batch.clone()).num_rows(), 0);
            let baseline = broker.inner.reserved();
            let (values, credit) = context.gauge_snapshot().unwrap();
            assert!(broker.inner.reserved() > baseline);
            for (pair, values) in schema.gauges.chunks_exact(2).zip(values.chunks_exact(2)) {
                let (size, ratio) = if pair[0].plan_node_id == 3 {
                    (5000, 1.0f64)
                } else {
                    (0, 0.0f64)
                };
                assert_eq!(values, &[size, ratio.to_bits() as i64]);
            }
            drop(values);
            drop(credit);
            assert_eq!(broker.inner.reserved(), baseline);
            assert!(context
                .snapshot_state(3, 0)
                .unwrap_err()
                .to_string()
                .contains("bundle control drain"));
            for event in [
                ControlEvent::Watermark(100),
                ControlEvent::BeforeCheckpoint(7),
                ControlEvent::EndInput,
            ] {
                let mut events = vec![(3, event)];
                if upper_mini {
                    events.push((5, event));
                }
                let output = drain(&context, batch.slice(0, 0), &events);
                let (values, credit) = context.gauge_snapshot().unwrap();
                assert!(values.iter().all(|value| *value == 0));
                drop(values);
                drop(credit);
                let count: usize = output.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(
                    count,
                    if event == ControlEvent::Watermark(100) {
                        5000
                    } else {
                        0
                    }
                );
                for batch in &output {
                    assert!(batch.num_rows() <= 4096);
                    assert_eq!(batch.schema().field(3).name(), OWNED_TIMESTAMP_V1);
                    assert_eq!(batch.column(3).null_count(), batch.num_rows());
                    assert!(batch
                        .column(4)
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .all(|kind| *kind == INSERT));
                    assert!(batch
                        .column(5)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .all(|ordinal| *ordinal == -1));
                }
            }
            let metrics = context.metric_snapshot().unwrap();
            assert_eq!(
                metrics,
                vec![
                    6, 5000, 5000, 5, 5000, 5000, 4, 5000, 5000, 3, 5000, 5000, 2, 5000, 5000, 1,
                    0, 5000
                ]
            );
            for id in [3, 5] {
                context.snapshot_state(id, 0).unwrap();
            }
            assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
            drop(context);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn drained_mini_state_restores_cross_backend_before_retractions() {
    let Some(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok() else {
        return;
    };
    for rocks_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(Broker {
            inner: TestBroker::new(64 << 20),
            transfers: AtomicUsize::new(0),
        });
        let memory = HostMemoryReservation::new(broker.clone(), "mini restore");
        let mut snapshots = Vec::<Vec<u8>>::new();
        for phase in 0..2 {
            let path = directory.path().join(format!("phase-{phase}"));
            let rocks = rocks_first != (phase != 0);
            let context = context(
                &mini_plan(false, false),
                &binding(rocks.then_some((plugin.as_str(), path.as_path()))),
                &memory,
            );
            for (group, bytes) in snapshots.iter().enumerate() {
                context.restore_state(3, group as u32, bytes).unwrap();
            }
            let kind = if phase == 0 { INSERT } else { DELETE };
            let batch = input(37, kind);
            assert_eq!(run(&context, batch.clone()).num_rows(), 0);
            let output = drain(
                &context,
                batch.slice(0, 0),
                &[(3, ControlEvent::BeforeCheckpoint(42))],
            );
            assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 37);
            for batch in &output {
                assert!(batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .all(|value| *value == kind));
            }
            snapshots = (0..16)
                .map(|group| context.snapshot_state(3, group).unwrap().to_vec())
                .collect();
            drop(output);
            drop(context);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[test]
fn cancelling_a_real_control_drain_requires_recovery_and_releases_output_on_drop() {
    let broker = Arc::new(Broker {
        inner: TestBroker::new(128 << 20),
        transfers: AtomicUsize::new(0),
    });
    let memory = HostMemoryReservation::new(broker.clone(), "cancel real bundle");
    let context = context(&mini_plan(false, false), &binding(None), &memory);
    let batch = input(5000, INSERT);
    run(&context, batch.clone());
    let mut stream = context
        .start_control(vec![batch.slice(0, 0)], &[(3, ControlEvent::EndInput)])
        .unwrap();
    // Empty ordinary input is still passed through the tree before the first control output.
    let output = loop {
        let output = context.runtime().block_on(stream.next()).unwrap().unwrap();
        if output.num_rows() > 0 {
            break output;
        }
    };
    assert_eq!(output.num_rows(), 2048);
    drop(stream);
    assert!(context.start(vec![batch]).is_err());
    assert!(context.snapshot_state(3, 0).is_err());
    // Flink resets its bundle count before calling the bundle function, also on failure.
    // Sampling the failed context is still safe and must not require an idle invocation.
    let (values, credit) = context.gauge_snapshot().unwrap();
    assert_eq!(values, vec![0, 0]);
    drop(values);
    drop(credit);
    drop(context);
    assert!(broker.inner.reserved() > 0);
    assert_eq!(output.column(0).len(), 2048);
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn measured_heap_of_shared_bundle_execution_fits_its_admitted_peak_and_live_owners() {
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
    for (rows, strings) in [0, 1, 32, 2500]
        .into_iter()
        .flat_map(|rows| [(rows, false), (rows, true)])
    {
        let broker = Arc::new(PeakBroker {
            inner: TestBroker::new(128 << 20),
            peak: AtomicUsize::new(0),
        });
        // Borrowed source buffers are created outside the measured native scope and remain live.
        let mut input = input(rows, INSERT);
        let mut plan = mini_plan(false, false);
        if strings {
            let varchar = proto::LogicalType {
                nullable: true,
                r#type: Some(proto::logical_type::Type::Varchar(proto::EmptyType {})),
            };
            let proto::operator::Operator::Calc(calc) =
                plan.root.as_mut().unwrap().operator.as_mut().unwrap()
            else {
                unreachable!()
            };
            let proto::operator::Operator::GroupAggregate(group) =
                calc.input.as_mut().unwrap().operator.as_mut().unwrap()
            else {
                unreachable!()
            };
            group.input_schema.as_mut().unwrap().fields[1].r#type = Some(varchar.clone());
            group.output_schema.as_mut().unwrap().fields[2].r#type = Some(varchar.clone());
            group.aggregate_calls[1].function = proto::AggregateFunction::Min as i32;
            group.aggregate_calls[1].input_type = Some(varchar.clone());
            group.aggregate_calls[1].output_type = Some(varchar);
            let mut fields = input.schema().fields().to_vec();
            fields[1] = Arc::new(Field::new("value", DataType::Utf8, true));
            let mut columns = input.columns().to_vec();
            columns[0] = Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % 31) as i64),
            ));
            columns[1] = Arc::new(StringArray::from_iter((0..rows).map(|row| {
                (row % 7 != 0).then(|| {
                    format!(
                        "{row:08}-{}",
                        "é".repeat(if row % 37 == 0 { 4096 } else { 8 })
                    )
                })
            })));
            input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        }
        let bindings = binding(None);
        let ((context, output), allocations) = crate::allocation_test_support::measure(|| {
            let memory = HostMemoryReservation::new(broker.clone(), "measured aggregate tree");
            let context = context(&plan, &bindings, &memory);
            run(&context, input.clone());
            let output = drain(
                &context,
                input.slice(0, 0),
                &[(3, ControlEvent::BeforeCheckpoint(1))],
            );
            (context, output)
        });
        assert!(allocations.live >= 0);
        assert!(
            allocations.live as usize <= broker.inner.reserved(),
            "rows={rows}, strings={strings}, allocations={allocations:?}, retained admission={}",
            broker.inner.reserved()
        );
        assert!(
            allocations.peak <= broker.peak.load(Ordering::Relaxed),
            "rows={rows}, strings={strings}, allocations={allocations:?}, peak admission={}",
            broker.peak.load(Ordering::Relaxed)
        );
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            if strings { rows.min(31) } else { rows }
        );
        drop(context);
        drop(output);
        assert_eq!(broker.inner.reserved(), 0);
    }
}
