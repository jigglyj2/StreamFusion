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
