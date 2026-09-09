// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use prost::Message;

fn plan(gap: i64) -> Vec<u8> {
    let mut plan = proto::NativePlan::decode(
        super::super::tests::plan(proto::WindowKind::Tumble, gap, 0, false).as_slice(),
    )
    .unwrap();
    let Some(proto::operator::Operator::WindowAggregate(window)) =
        &mut plan.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    window.processing_time = true;
    window.input_schema.as_mut().unwrap().fields[0]
        .r#type
        .as_mut()
        .unwrap()
        .nullable = true;
    window.output_schema.as_mut().unwrap().fields[0]
        .r#type
        .as_mut()
        .unwrap()
        .nullable = true;
    let clock = window.input_schema.as_mut().unwrap().fields[1]
        .r#type
        .as_mut()
        .unwrap();
    clock.nullable = true;
    clock.r#type = Some(proto::logical_type::Type::TimestampLtz(
        proto::PrecisionType { precision: 3 },
    ));
    plan.encode_to_vec()
}

fn window(
    bytes: &[u8],
    broker: Arc<TestBroker>,
    rocks: Option<&std::path::Path>,
    first: u32,
    last: u32,
) -> SharedProcessingWindows {
    let memory = HostMemoryReservation::new(broker, "processing-time state test");
    let timer = memory.sibling("processing-time timers");
    let scratch = memory.sibling("processing-time workspace");
    let state: Box<dyn KeyedState> = match rocks {
        Some(directory) => Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                directory,
                first,
                last,
                8 << 20,
                &memory,
            )
            .unwrap(),
        ),
        None => Box::new(crate::state::OrderedMemoryKeyedState::new(first, last, memory).unwrap()),
    };
    let kernel =
        WindowAggregateProcessor::with_state(bytes, 128, first, last, state, timer, scratch)
            .unwrap();
    SharedProcessingWindows::new(kernel, 3 << 20, 32 << 10).unwrap()
}

fn backends() -> Vec<bool> {
    if std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_ok() {
        vec![false, true]
    } else {
        vec![false]
    }
}

fn push(window: &mut SharedProcessingWindows, keys: Vec<Option<i64>>, clocks: Vec<i64>) {
    let size = keys.len();
    let batch = RecordBatch::try_new(
        window.input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(keys)) as ArrayRef,
            arrow::array::new_null_array(window.input_schema.field(1).data_type(), size),
        ],
    )
    .unwrap();
    window.process(batch, Int64Array::from(clocks)).unwrap();
}

fn push_count(window: &mut SharedProcessingWindows, key: Option<i64>, count: usize, clock: i64) {
    push(window, vec![key; count], vec![clock; count]);
}

fn output(batch: &RecordBatch) -> Vec<(Option<i64>, i64, i64, i64)> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let starts = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let ends = batch
        .column(3)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                keys.is_valid(row).then(|| keys.value(row)),
                counts.value(row),
                starts.value(row),
                ends.value(row),
            )
        })
        .collect()
}

fn fire(window: &mut SharedProcessingWindows, deadline: i64) -> Vec<(Option<i64>, i64, i64, i64)> {
    window
        .control(ControlEvent::ProcessingTime(deadline))
        .unwrap();
    let mut result = Vec::new();
    while window.next_timer().is_some_and(|next| next <= deadline) {
        result.extend(output(&window.fire(deadline).unwrap()));
    }
    result
}

#[test]
fn repeated_and_backward_callbacks_match_flink_count_zero_and_checkpoint_buffer_visibility() {
    for rocks in backends() {
        for gap in [1000, 10000, 37000] {
            let directory = tempfile::tempdir().unwrap();
            let broker = Arc::new(TestBroker::new(64 << 20));
            let mut window = window(
                &plan(gap),
                broker.clone(),
                rocks.then_some(directory.path()),
                0,
                127,
            );
            push_count(&mut window, None, 2, gap + 1);
            assert_eq!(window.next_timer(), Some(2 * gap - 1));
            assert_eq!(window.slices.kernel().state_write_batches, 0);
            window.control(ControlEvent::Watermark(i64::MAX)).unwrap();
            window.control(ControlEvent::EndInput).unwrap();
            assert_eq!(window.slices.kernel().state_write_batches, 0);
            assert_eq!(fire(&mut window, 2 * gap - 1), [(None, 2, gap, 2 * gap)]);
            push_count(&mut window, None, 3, 1);
            assert_eq!(fire(&mut window, gap - 1), [(None, 0, 0, gap)]);
            window.control(ControlEvent::BeforeCheckpoint(9)).unwrap();
            // Publishing old buffered updates must not recreate an already-fired timer.
            assert_eq!(window.next_timer(), None);
            push_count(&mut window, None, 5, 1);
            assert_eq!(fire(&mut window, gap - 1), [(None, 3, 0, gap)]);
            window.control(ControlEvent::BeforeCheckpoint(10)).unwrap();
            push_count(&mut window, None, 1, 1);
            assert_eq!(fire(&mut window, gap - 1), [(None, 5, 0, gap)]);
            assert_eq!(window.slices.kernel().late_records_dropped, 0);
            assert_eq!(window.slices.kernel().current_event_time, i64::MAX);
            drop(window);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn per_record_assignment_publishes_future_partials_but_only_fires_due_timers_in_bounded_batches() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut window = window(
            &plan(1000),
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        push(
            &mut window,
            vec![Some(7), Some(7), None],
            vec![-1, 1001, 999],
        );
        let mut actual = fire(&mut window, 999);
        actual.sort();
        assert_eq!(actual, [(None, 1, 0, 1000), (Some(7), 1, -1000, 0)]);
        assert_eq!(window.next_timer(), Some(1999));
        assert_eq!(fire(&mut window, 1999), [(Some(7), 1, 1000, 2000)]);
        let keys = (0..3000).map(Some).collect::<Vec<_>>();
        push(&mut window, keys.clone(), vec![2001; keys.len()]);
        window.control(ControlEvent::ProcessingTime(2999)).unwrap();
        let mut actual = Vec::new();
        while window.next_timer().is_some() {
            let batch = window.fire(2999).unwrap();
            assert!(batch.num_rows() <= super::super::shared_slices::OUTPUT_ROWS);
            actual.extend(output(&batch));
        }
        actual.sort();
        assert_eq!(
            actual,
            keys.into_iter()
                .map(|key| (key, 1, 2000, 3000))
                .collect::<Vec<_>>()
        );
        let kernel = window.slices.kernel();
        assert!(kernel.state_read_batches < 16 && kernel.state_write_batches < 16);
        drop(window);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn restore_switches_backends_and_rescales_absolute_timers_independently_of_watermarks() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(128 << 20));
        let bytes = plan(1000);
        let mut source = window(
            &bytes,
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        let keys = (0..64).map(Some).chain([None]).collect::<Vec<_>>();
        push(&mut source, keys.clone(), vec![1; keys.len()]);
        source.control(ControlEvent::Watermark(i64::MAX)).unwrap();
        source.control(ControlEvent::BeforeCheckpoint(11)).unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot(group).unwrap())
            .collect::<Vec<_>>();
        drop(source);
        let mut actual = Vec::new();
        for (first, last) in [(0, 63), (64, 127)] {
            let restored_directory = tempfile::tempdir().unwrap();
            let mut restored = window(
                &bytes,
                broker.clone(),
                (!rocks && backends().len() > 1).then_some(restored_directory.path()),
                first,
                last,
            );
            for group in first..=last {
                restored
                    .restore(group, &snapshots[group as usize], i64::MAX)
                    .unwrap();
            }
            assert_eq!(restored.next_timer(), Some(999));
            actual.extend(fire(&mut restored, 999));
            assert_eq!(restored.next_timer(), None);
        }
        actual.sort();
        let mut expected = keys
            .into_iter()
            .map(|key| (key, 1, 0, 1000))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected);
        drop(snapshots);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn malformed_clock_is_rejected_before_state_or_timer_mutation() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let mut window = window(&plan(1000), broker.clone(), None, 0, 127);
    let batch = RecordBatch::new_empty(window.input_schema.clone());
    assert!(window.process(batch, Int64Array::from(vec![None])).is_err());
    assert_eq!(window.next_timer(), None);
    assert_eq!(window.slices.kernel().state_read_batches, 0);
    push_count(&mut window, Some(9), 2, 1);
    assert!(window
        .snapshot(0)
        .unwrap_err()
        .to_string()
        .contains("pre-checkpoint"));
    assert_eq!(fire(&mut window, 999), [(Some(9), 2, 0, 1000)]);
    drop(window);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn checkpoint_identity_covers_raw_columns_omitted_from_the_partial_layout() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let original = plan(1000);
    let mut source = window(&original, broker.clone(), None, 0, 127);
    source.control(ControlEvent::BeforeCheckpoint(1)).unwrap();
    let snapshot = source.snapshot(0).unwrap();
    drop(source);
    let mut changed = proto::NativePlan::decode(original.as_slice()).unwrap();
    let Some(proto::operator::Operator::WindowAggregate(window_plan)) =
        &mut changed.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    // An unused raw column changes Flink's buffering geometry, though the internal
    // partial schema and its aggregate/grouping columns remain exactly the same.
    window_plan
        .input_schema
        .as_mut()
        .unwrap()
        .fields
        .push(super::super::tests::field(
            "extra",
            super::super::tests::logical_bigint(true),
        ));
    let mut restored = window(&changed.encode_to_vec(), broker.clone(), None, 0, 127);
    assert!(restored
        .restore(0, &snapshot, i64::MIN)
        .unwrap_err()
        .to_string()
        .contains("matching versioned"));
    drop(restored);
    drop(snapshot);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn denied_batch_workspace_poisoning_releases_all_credit_when_the_context_closes() {
    let limit = 4 << 20;
    let broker = Arc::new(TestBroker::new(limit));
    let mut window = window(&plan(1000), broker.clone(), None, 0, 127);
    let mut occupied = HostMemoryReservation::new(broker.clone(), "other Flink consumer");
    occupied.resize(limit - broker.reserved() - 1024).unwrap();
    let batch = RecordBatch::try_new(
        window.input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1); 1000])) as ArrayRef,
            arrow::array::new_null_array(window.input_schema.field(1).data_type(), 1000),
        ],
    )
    .unwrap();
    assert!(window
        .process(batch, Int64Array::from(vec![1; 1000]))
        .is_err());
    assert!(window.require_healthy().is_err());
    assert!(window.control(ControlEvent::BeforeCheckpoint(1)).is_err());
    drop(occupied);
    drop(window);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn global_group_uses_the_same_partition_identity_for_arrival_timers_and_published_state() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let mut native = proto::NativePlan::decode(plan(1000).as_slice()).unwrap();
        let Some(proto::operator::Operator::WindowAggregate(aggregate)) =
            &mut native.root.as_mut().unwrap().operator
        else {
            unreachable!()
        };
        aggregate.grouping_indices.clear();
        aggregate.output_schema.as_mut().unwrap().fields.remove(0);
        let mut window = window(
            &native.encode_to_vec(),
            broker.clone(),
            rocks.then_some(directory.path()),
            0,
            127,
        );
        push(&mut window, vec![Some(7), Some(9), None], vec![1, 2, 3]);
        assert_eq!(window.slices.kernel().timer_registrations, 1);
        window.control(ControlEvent::ProcessingTime(999)).unwrap();
        let output = window.fire(999).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(window.next_timer(), None);
        drop(window);
        drop(output);
        assert_eq!(broker.reserved(), 0);
    }
}
