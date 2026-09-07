// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::execution_context::NativeExecutionContext;
use crate::memory_pool::MemoryReservationBroker;
use crate::planner::operators::envelope::{INPUT_ROW, OWNED_TIMESTAMP_V1, ROW_KIND};
use crate::planner::persistent::control::ControlEvent;
use arrow::array::Int32Array;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

mod composition;

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

fn tree(trigger: u64, changelog: bool) -> proto::NativePlan {
    let mut plan = proto::NativePlan::decode(plan(trigger, changelog).as_slice()).unwrap();
    plan.protocol_version = 2;
    let mut local = plan.root.take().unwrap();
    local.plan_node_id = 3;
    let Some(proto::operator::Operator::LocalGroupAggregate(local_plan)) = local.operator.as_mut()
    else {
        unreachable!()
    };
    local_plan.input = Some(Box::new(calc(
        2,
        proto::Operator {
            plan_node_id: 1,
            operator: Some(proto::operator::Operator::Input(proto::Input::default())),
            ..Default::default()
        },
        2,
    )));
    plan.root = Some(calc(4, local, 2));
    plan
}

fn input(keys: Vec<i64>, values: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    let batch = batch(keys, values, Some(kinds));
    let mut fields = batch.schema().fields().to_vec();
    fields[2] = Arc::new(Field::new(ROW_KIND, DataType::Int8, false));
    fields.push(Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from_iter_values(
        0..batch.num_rows() as i32,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    transfers: AtomicUsize,
    peak: AtomicUsize,
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let admitted = self.inner.try_reserve(bytes)?;
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        Ok(admitted)
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
fn memory() -> (Arc<Broker>, HostMemoryReservation) {
    let broker = Arc::new(Broker {
        inner: TestBroker::new(256 << 20),
        transfers: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let memory = HostMemoryReservation::new(broker.clone(), "local tree test");
    (broker, memory)
}
fn run(
    context: &Arc<NativeExecutionContext>,
    input: RecordBatch,
    controls: &[(u64, ControlEvent)],
) -> Vec<RecordBatch> {
    let mut stream = if controls.is_empty() {
        context.start(vec![input])
    } else {
        context.start_control(vec![input], controls)
    }
    .unwrap();
    let mut output = Vec::new();
    while let Some(batch) = context.runtime().block_on(stream.next()) {
        output.push(batch.unwrap());
    }
    output
}
fn payloads(batches: &[RecordBatch]) -> Vec<(i64, Vec<u8>)> {
    batches
        .iter()
        .flat_map(|batch| {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (keys.value(row), values.value(row).to_vec()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn local_registers_before_lowering_and_retains_bundle_until_explicit_control() {
    let (broker, memory) = memory();
    let context = Arc::new(
        NativeExecutionContext::new(
            &tree(100_000, true).encode_to_vec(),
            memory.datafusion_pool(256 << 20),
        )
        .unwrap(),
    );
    let (wire, credit) = context.control_capabilities().unwrap();
    let capabilities = proto::NativeControlCapabilities::decode(wire.as_slice()).unwrap();
    assert_eq!(capabilities.stages.len(), 1);
    let stage = &capabilities.stages[0];
    assert_eq!(stage.plan_node_id, 3);
    assert!(stage.watermark && stage.before_checkpoint && stage.end_input);
    drop((wire, credit));
    let (wire, credit) = context.gauge_schema().unwrap();
    let gauges = proto::NativeGaugeSchema::decode(wire.as_slice()).unwrap();
    assert_eq!(
        gauges
            .gauges
            .iter()
            .map(|g| (g.plan_node_id, g.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(3, "bundleSize"), (3, "bundleRatio")]
    );
    drop((wire, credit));
    for control in [
        ControlEvent::Watermark(11),
        ControlEvent::BeforeCheckpoint(7),
        ControlEvent::EndInput,
    ] {
        let batch = input((0..5000).collect(), (0..5000).collect(), vec![0; 5000]);
        assert_eq!(payloads(&run(&context, batch.clone(), &[])).len(), 0);
        assert_eq!(
            context.gauge_snapshot().unwrap().0,
            vec![5000, 1.0f64.to_bits() as i64]
        );
        // Invocation EOF did not flush. A task-local bundle has no fake keyed snapshot.
        assert!(context
            .snapshot_state(3, 0)
            .unwrap_err()
            .to_string()
            .contains("no shared snapshot"));
        let output = run(&context, batch.slice(0, 0), &[(3, control)]);
        assert!(output.iter().all(|batch| batch.num_rows() <= 2048));
        assert_eq!(payloads(&output).len(), 5000);
        for batch in &output {
            assert_eq!(batch.schema().field(2).name(), OWNED_TIMESTAMP_V1);
            assert_eq!(batch.column(2).null_count(), batch.num_rows());
            assert!(batch
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values()
                .iter()
                .all(|kind| *kind == 0));
            assert!(batch
                .column(4)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .all(|ordinal| *ordinal == -1));
        }
        assert_eq!(context.gauge_snapshot().unwrap().0, vec![0, 0]);
    }
    assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
    drop(context);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn count_trigger_and_retractions_match_existing_local_kernel_byte_for_byte() {
    for trigger in [1, 7, 100_000] {
        let (broker, memory) = memory();
        let context = Arc::new(
            NativeExecutionContext::new(
                &tree(trigger, true).encode_to_vec(),
                memory.datafusion_pool(256 << 20),
            )
            .unwrap(),
        );
        let mut oracle = processor(trigger, true);
        for phase in [0, 1] {
            let keys: Vec<_> = (0..130).map(|i| i % 11).collect();
            let values: Vec<_> = (0..130).collect();
            let kinds: Vec<_> = (0..130)
                .map(|i| if (i + phase) % 3 == 0 { 3 } else { 0 })
                .collect();
            let expected = oracle
                .process_arrow(batch(keys.clone(), values.clone(), Some(kinds.clone())))
                .unwrap();
            let input = input(keys, values, kinds);
            assert_eq!(
                payloads(&run(&context, input.clone(), &[])),
                payloads(&[expected])
            );
            let expected = oracle.finish_bundle().unwrap();
            assert_eq!(
                payloads(&run(
                    &context,
                    input.slice(0, 0),
                    &[(3, ControlEvent::Watermark(phase))]
                )),
                payloads(&[expected])
            );
        }
        assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
        drop(context);
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn cancellation_requires_recovery_and_output_lease_outlives_tree() {
    let (broker, memory) = memory();
    let context = Arc::new(
        NativeExecutionContext::new(
            &tree(100_000, true).encode_to_vec(),
            memory.datafusion_pool(256 << 20),
        )
        .unwrap(),
    );
    let input = input((0..5000).collect(), (0..5000).collect(), vec![0; 5000]);
    drop(run(&context, input.clone(), &[]));
    let mut stream = context
        .start_control(vec![input.slice(0, 0)], &[(3, ControlEvent::EndInput)])
        .unwrap();
    let output = loop {
        let batch = context.runtime().block_on(stream.next()).unwrap().unwrap();
        if batch.num_rows() > 0 {
            break batch;
        }
    };
    drop(stream);
    assert!(context
        .require_idle()
        .unwrap_err()
        .to_string()
        .contains("recovery"));
    drop(context);
    assert!(broker.inner.reserved() >= output.get_array_memory_size());
    assert_eq!(payloads(&[output.clone()]).len(), 2048);
    drop(output);
    assert_eq!(broker.inner.reserved(), 0);
}

#[test]
fn native_constructor_count_and_control_paths_cover_observed_heap() {
    use crate::allocation_test_support::measure;
    for trigger in [7, 100_000] {
        let (broker, memory) = memory();
        let plan = tree(trigger, true).encode_to_vec();
        let input = input((0..5000).collect(), (0..5000).collect(), vec![0; 5000]);
        let ((context, output), observed) = measure(|| {
            let context = Arc::new(
                NativeExecutionContext::new(&plan, memory.datafusion_pool(256 << 20)).unwrap(),
            );
            let mut output = run(&context, input.clone(), &[]);
            output.extend(run(
                &context,
                input.slice(0, 0),
                &[(3, ControlEvent::EndInput)],
            ));
            (context, output)
        });
        assert!(
            observed.peak <= broker.peak.load(Ordering::Relaxed),
            "trigger={trigger}: {observed:?}"
        );
        assert!(
            observed.live.max(0) as usize <= broker.inner.reserved(),
            "trigger={trigger}: {observed:?}, credit={}",
            broker.inner.reserved()
        );
        assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
        drop((context, output));
        assert_eq!(broker.inner.reserved(), 0);
    }
}

#[test]
fn invalid_input_contract_does_not_mutate_an_existing_bundle() {
    for (changelog, invalid) in [(false, 3), (true, 7)] {
        let (_, memory) = memory();
        let context = Arc::new(
            NativeExecutionContext::new(
                &tree(100_000, changelog).encode_to_vec(),
                memory.datafusion_pool(256 << 20),
            )
            .unwrap(),
        );
        drop(run(&context, input(vec![1], vec![10], vec![0]), &[]));
        let mut stream = context
            .start(vec![input(vec![2, 3], vec![20, 30], vec![0, invalid])])
            .unwrap();
        assert!(context
            .runtime()
            .block_on(stream.next())
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("RowKind"));
        drop(stream);
        assert!(context.require_idle().is_err());
        assert_eq!(
            context.gauge_snapshot().unwrap().0,
            vec![1, 1.0f64.to_bits() as i64]
        );
    }
}
