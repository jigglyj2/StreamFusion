// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::planner::operators::group_aggregate::Accumulator;
use prost::Message;

pub(super) fn plan() -> Vec<u8> {
    use super::super::tests::{field, logical_bigint, plan};
    let mut native =
        proto::NativePlan::decode(plan(proto::WindowKind::Hop, 6000, 2000, false).as_slice())
            .unwrap();
    let Some(proto::operator::Operator::WindowAggregate(aggregate)) =
        &mut native.root.as_mut().unwrap().operator
    else {
        unreachable!()
    };
    aggregate.time_attribute_index = 0;
    aggregate.partial_accumulator_index = Some(1);
    aggregate.partial_window_start_index = Some(2);
    aggregate.partial_slice_end_index = Some(3);
    aggregate.partial_windows_are_slices = true;
    aggregate.input_schema = Some(proto::Schema {
        fields: vec![
            field("key", logical_bigint(false)),
            field(
                "accumulator",
                proto::LogicalType {
                    nullable: false,
                    r#type: Some(proto::logical_type::Type::Binary(proto::EmptyType {})),
                },
            ),
            field("window_start", logical_bigint(false)),
            field("slice_end", logical_bigint(false)),
        ],
    });
    native.encode_to_vec()
}

fn processor(
    broker: Arc<TestBroker>,
    rocks: Option<&std::path::Path>,
    first: u32,
    last: u32,
) -> SharedSlices {
    processor_with_plan(&plan(), broker, rocks, first, last)
}

fn processor_with_plan(
    bytes: &[u8],
    broker: Arc<TestBroker>,
    rocks: Option<&std::path::Path>,
    first: u32,
    last: u32,
) -> SharedSlices {
    let reservation = HostMemoryReservation::new(broker.clone(), "shared slice state");
    let timer = reservation.sibling("shared slice timers");
    let scratch = reservation.sibling("shared slice workspace");
    let state: Box<dyn KeyedState> = match rocks {
        Some(directory) => Box::new(
            RocksPluginKeyedState::open_for_owner(
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                directory,
                first,
                last,
                8 << 20,
                &reservation,
            )
            .unwrap(),
        ),
        None => {
            Box::new(crate::state::OrderedMemoryKeyedState::new(first, last, reservation).unwrap())
        }
    };
    SharedSlices::new(
        WindowAggregateProcessor::with_state(bytes, 128, first, last, state, timer, scratch)
            .unwrap(),
    )
    .unwrap()
}

pub(super) fn batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    let encoded = rows
        .iter()
        .map(|&(_, count, _)| {
            encode_state(&AccumulatorState {
                row_count: count,
                accumulators: vec![Accumulator::Count(count)],
            })
        })
        .collect::<Vec<_>>();
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
        ),
        (
            "accumulator",
            Arc::new(BinaryArray::from_iter_values(encoded)) as ArrayRef,
        ),
        (
            "window_start",
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.2 - 2000),
            )) as ArrayRef,
        ),
        (
            "slice_end",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn output(batch: &RecordBatch) -> Vec<(i64, i64, i64, i64)> {
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
                keys.value(row),
                counts.value(row),
                starts.value(row),
                ends.value(row),
            )
        })
        .collect()
}
fn slice_count(processor: &SharedSlices) -> usize {
    let mut count = 0;
    for group in processor.kernel.timers.key_group_range() {
        processor
            .kernel
            .state
            .visit_key_group(group, 256, 1 << 20, &mut |page| {
                count += page
                    .iter()
                    .filter(|(key, _)| key.first() == Some(&STATE_NAMESPACE))
                    .count();
                Ok(())
            })
            .unwrap();
    }
    count
}
fn backends() -> Vec<bool> {
    if std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").is_ok() {
        vec![false, true]
    } else {
        vec![false]
    }
}

mod attached;
mod checkpoint;
mod generated;

#[test]
fn borrowed_partial_slices_do_not_reserve_the_parent_arrow_buffers_again() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new((if rocks { 9 } else { 1 }) << 20));
        let mut window = processor(broker.clone(), rocks.then_some(directory.path()), 0, 127);
        let parent = batch(&(0..16_384).map(|key| (key, 1, 2000)).collect::<Vec<_>>());
        let borrowed = parent.slice(8100, 32);
        window.process(&borrowed).unwrap();
        assert_eq!(slice_count(&window), 32);
        for end in [2000, 4000, 6000] {
            let mut actual = output(&window.advance(end - 1).unwrap());
            actual.sort();
            let expected = (8100..8132)
                .map(|key| (key, 1, end - 6000, end))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        drop(window);
        assert_eq!(broker.reserved(), 0);
        // The producer's retained Arrow batch remains valid after the window is gone.
        assert_eq!(borrowed.num_rows(), 32);
        assert_eq!(parent.num_rows(), 16_384);
    }
}

#[test]
fn shared_slices_match_the_flink_global_hop_late_input_and_empty_timer_contract() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut window = processor(broker.clone(), rocks.then_some(directory.path()), 0, 127);
        window.process(&batch(&[(1, 2, 2000)])).unwrap();
        assert_eq!(slice_count(&window), 1);
        assert_eq!(window.kernel.timers.timer_count(TimerDomain::EventTime), 1);
        assert_eq!(
            output(&window.advance(1999).unwrap()),
            [(1, 2, -4000, 2000)]
        );
        window.process(&batch(&[(2, 3, 2000)])).unwrap();
        assert_eq!(window.kernel.late_records_dropped, 0);
        assert_eq!(slice_count(&window), 2);
        for end in [4000, 6000] {
            let mut actual = output(&window.advance(end - 1).unwrap());
            actual.sort();
            assert_eq!(actual, [(1, 2, end - 6000, end), (2, 3, end - 6000, end)]);
        }
        assert_eq!(slice_count(&window), 0);
        assert_eq!(window.kernel.timers.timer_count(TimerDomain::EventTime), 2);
        assert_eq!(window.advance(7999).unwrap().num_rows(), 0);
        assert_eq!(window.next_timer(), None);
        window.process(&batch(&[(3, 11, 2000)])).unwrap();
        assert_eq!(window.kernel.late_records_dropped, 1);
        assert_eq!(slice_count(&window), 0);
        drop(window);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn a_watermark_jump_does_not_register_already_pending_windows_twice() {
    for rocks in backends() {
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(256 << 20));
        let mut window = processor(broker.clone(), rocks.then_some(directory.path()), 0, 127);
        window
            .process(&batch(&[(1, 1, 2000), (1, 2, 4000)]))
            .unwrap();
        assert_eq!(slice_count(&window), 2);
        let mut actual = Vec::new();
        while window.next_timer().is_some() {
            actual.extend(output(&window.advance(i64::MAX).unwrap()));
        }
        assert_eq!(
            actual,
            [
                (1, 1, -4000, 2000),
                (1, 3, -2000, 4000),
                (1, 3, 0, 6000),
                (1, 2, 2000, 8000)
            ]
        );
        assert_eq!(slice_count(&window), 0);
        drop(window);
        assert_eq!(broker.reserved(), 0);
    }
}
