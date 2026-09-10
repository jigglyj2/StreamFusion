// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn historical_counts_for_inactive_filters_fit_the_batch_membership_allowance() {
    let broker = Arc::new(TestBroker::new(64 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "filtered history memory");
    let mut wire = proto::NativePlan::decode(plan(false).as_slice()).unwrap();
    let proto::operator::Operator::GroupAggregate(group) =
        wire.root.as_mut().unwrap().operator.as_mut().unwrap()
    else {
        unreachable!()
    };
    // One active unfiltered call and fifteen filters that become inactive in the next batch.
    // Their historical memberships must still be loaded and preserved for each selected value.
    for _ in 0..14 {
        group
            .aggregate_calls
            .insert(1, group.aggregate_calls[1].clone());
    }
    let mut processor =
        GroupAggregateProcessor::new(&wire.encode_to_vec(), 16, 0, 15, owner.sibling("state"))
            .unwrap();
    let rows = (0..1000)
        .map(|value| (7, Some(value), Some(true), INSERT))
        .collect::<Vec<_>>();
    drop(processor.process_arrow(input(false, &rows)).unwrap());
    let rows = (0..1000)
        .map(|value| (7, Some(value), Some(false), INSERT))
        .collect::<Vec<_>>();
    let incoming = input(false, &rows);
    let key = processor.state_key(&incoming, 0).unwrap();
    let header = processor
        .state
        .get_batch(
            &[StateKeyRef {
                key_group: key.key_group,
                key: &key.key,
            }],
            &owner,
        )
        .unwrap()[0]
        .as_ref()
        .unwrap()
        .to_vec();
    let layout = processor.membership_layout.as_ref().unwrap();
    let groups = vec![0; incoming.num_rows()];
    let accumulates = vec![true; incoming.num_rows()];
    let keys = [key];
    let mut base_credit = owner.sibling("incoming accumulator growth");
    base_credit
        .resize(processor.accumulator_input_admission(1, &incoming).unwrap())
        .unwrap();
    let before = broker.reserved();
    let ((members, staged), observed) = crate::allocation_test_support::measure(|| {
        let mut staged = vec![Some(layout.decode(&header, &processor.calls).unwrap())];
        let members = layout
            .load(
                &processor.calls,
                processor.state.as_ref(),
                &incoming,
                &keys,
                &groups,
                &accumulates,
                &mut staged,
                &[true],
                &[Mode::Counted],
                &owner,
            )
            .unwrap();
        (members, staged)
    });
    let admitted = broker.reserved() - before + base_credit.size();
    assert!(
        observed.peak <= admitted,
        "observed={observed:?} admitted={admitted}"
    );
    for accumulator in &staged[0].as_ref().unwrap().accumulators[..16] {
        let Accumulator::DistinctCount { values, .. } = accumulator else {
            unreachable!()
        };
        assert_eq!(values.len(), 1000);
        assert!(values.values().all(|&count| count == 1));
    }
    drop((members, staged, base_credit, processor, owner));
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn completed_membership_work_releases_directories_but_keeps_dirty_buffers_charged() {
    for changed in [false, true] {
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "membership phase ownership");
        let (mut processor, _directory, _) = processor(false, true, &owner);
        let rows = (0..2048)
            .map(|value| (7, Some(value), Some(true), INSERT))
            .collect::<Vec<_>>();
        let incoming = input(true, &rows);
        drop(processor.process_arrow(incoming.clone()).unwrap());
        let key = processor.state_key(&incoming, 0).unwrap();
        let stored = processor
            .state
            .get_batch(
                &[StateKeyRef {
                    key_group: key.key_group,
                    key: &key.key,
                }],
                &owner,
            )
            .unwrap();
        let layout = processor.membership_layout.as_ref().unwrap();
        let mut staged = vec![Some(
            layout
                .decode(stored[0].as_ref().unwrap(), &processor.calls)
                .unwrap(),
        )];
        drop(stored);
        let before_load = broker.reserved();
        let mut members = layout
            .load(
                &processor.calls,
                processor.state.as_ref(),
                &incoming,
                &[key],
                &vec![0; incoming.num_rows()],
                &vec![true; incoming.num_rows()],
                &mut staged,
                &[true],
                &[Mode::Counted],
                &owner,
            )
            .unwrap();
        if changed {
            for accumulator in &mut staged[0].as_mut().unwrap().accumulators[..2] {
                let Accumulator::DistinctCount { values, .. } = accumulator else {
                    unreachable!()
                };
                for value in values.values_mut() {
                    *value += 1;
                }
            }
        }
        let mut mutations = Vec::new();
        mutations.extend(members.mutations(layout, &staged, &[true], &[Mode::Counted]));
        assert_eq!(mutations.len(), if changed { rows.len() } else { 0 });
        drop(staged);
        let before = broker.reserved();
        let buffers = mutations.iter().fold(
            mutations.capacity() * std::mem::size_of::<StateMutation>(),
            |bytes, mutation| {
                bytes
                    + mutation.key.key.capacity()
                    + mutation.value.as_ref().map_or(0, Vec::capacity)
            },
        );
        members.finish_computation().unwrap();
        assert!(broker.reserved() - before_load >= buffers);
        assert!(before - broker.reserved() > 1 << 20);
        for mutation in &mutations {
            assert_eq!(
                codec::decode_members(mutation.value.as_ref().unwrap(), 2, Mode::Counted).unwrap(),
                vec![2, 2]
            );
        }
        // The backend flush uses these moved buffers before their batch owner is released.
        processor.state.write_batch(mutations).unwrap();
        drop((members, processor, owner));
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn wide_filtered_counts_fit_output_after_releasing_finished_workspace() {
    let mut wire = proto::NativePlan::decode(plan(false).as_slice()).unwrap();
    let Some(proto::operator::Operator::GroupAggregate(group)) =
        wire.root.as_mut().unwrap().operator.as_mut()
    else {
        unreachable!()
    };
    for _ in 0..14 {
        group
            .aggregate_calls
            .insert(1, group.aggregate_calls[1].clone());
    }
    let wire = wire.encode_to_vec();
    let rows = (0..4096)
        .map(|value| (value % 64, Some(value), Some(true), INSERT))
        .collect::<Vec<_>>();
    for rocks in backends() {
        let broker = Arc::new(TestBroker::new(32 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "wide count output phases");
        let (mut actual, _actual_directory, io) = processor_with_plan(rocks, &owner, &wire);
        let reference_owner = HostMemoryReservation::new(
            Arc::new(TestBroker::new(64 << 20)),
            "chunked count reference",
        );
        let (mut expected, _expected_directory, _) =
            processor_with_plan(rocks, &reference_owner, &wire);
        let output = actual.process_arrow(input(false, &rows)).unwrap();
        let batches = rows
            .chunks(256)
            .map(|rows| expected.process_arrow(input(false, rows)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            output,
            arrow::compute::concat_batches(&output.schema(), batches.iter()).unwrap()
        );
        assert_eq!(io.read_batches.load(Ordering::Relaxed), 1);
        assert_eq!(io.write_batches.load(Ordering::Relaxed), 1);
        for key_group in 0..16 {
            assert_eq!(
                actual.snapshot_key_group(key_group).unwrap(),
                expected.snapshot_key_group(key_group).unwrap()
            );
        }
        drop((output, batches, actual, owner));
        assert_eq!(broker.reserved(), 0);
    }
}
