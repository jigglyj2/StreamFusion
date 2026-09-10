// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use crate::memory_pool::tests_support::TestBroker;
use crate::state::observed_tests::{Io, Observed};
use arrow::array::StringArray;
use prost::Message;
use std::sync::atomic::Ordering as AtomicOrdering;

fn sources(value: &str) -> CandidateSources {
    CandidateSources {
        batches: vec![Arc::new(
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(StringArray::from(vec![value])) as ArrayRef,
            )])
            .unwrap(),
        )],
        orders: None,
    }
}

fn event() -> OutputEvent {
    OutputEvent {
        candidate: CandidateRef {
            source: 0,
            row: 0,
            sequence: 0,
            kind: INSERT,
        },
        rank: 1,
        kind: UPDATE_AFTER,
    }
}

#[test]
fn repeated_payload_is_admitted_before_gather_and_credit_survives_output_handoff() {
    let source = sources(&"x".repeat(64 << 10));
    let broker = Arc::new(TestBroker::new(16 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "output test");
    let mut output = OutputBuffer::new(&source, &owner, true).unwrap();
    for _ in 0..32 {
        output.push(event()).unwrap();
    }
    output.reserve_payload(&source).unwrap();
    assert!(broker.reserved() >= 8 << 20);
    let (events, mut memory) = output.into_parts();
    let batch = output_batch(
        &proto::TopN::default(),
        &source[0].schema(),
        &source,
        events,
    )
    .unwrap();
    let mut credit = owner.sibling("output edge");
    credit
        .grow_from(memory.as_mut().unwrap(), batch.get_array_memory_size())
        .unwrap();
    drop(memory);
    let batch = crate::memory_pool::arrow_lease::host_batch(batch, credit).unwrap();
    let retained = batch.clone();
    drop(batch);
    assert!(broker.reserved() >= 2 << 20);
    drop(retained);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn descriptor_growth_and_amplified_payload_refuse_budget_without_large_gather() {
    let source = sources(&"x".repeat(64 << 10));
    let broker = Arc::new(TestBroker::new(128 << 10));
    let owner = HostMemoryReservation::new(broker.clone(), "descriptor denial");
    let mut output = OutputBuffer::new(&source, &owner, true).unwrap();
    let mut denied = false;
    for _ in 0..1024 {
        if output.push(event()).is_err() {
            denied = true;
            break;
        }
    }
    assert!(denied);
    assert!(output.len() < 1024);
    drop(output);
    assert_eq!(broker.reserved(), 0);
    let broker = Arc::new(TestBroker::new(8 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "payload denial");
    let (_, allocation) = crate::allocation_test_support::measure(|| {
        let mut output = OutputBuffer::new(&source, &owner, true).unwrap();
        for _ in 0..400 {
            output.push(event()).unwrap();
        }
        assert!(output.reserve_payload(&source).is_err());
    });
    // No 25 MiB gather is materialized before its reservation is denied.
    assert!(allocation.peak < 1 << 20, "peak {}", allocation.peak);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn amplified_rank_changelog_refusal_precedes_writes_on_both_state_backends() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks in [false, true] {
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(64 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "Top-N output admission");
        let io = Arc::new(Io::default());
        let state: Box<dyn KeyedState> = if rocks {
            Box::new(
                RocksPluginKeyedState::open_configured(
                    &proto::NativeRocksDbState {
                        plugin_path: plugin.clone().unwrap(),
                        database_path: directory.path().to_str().unwrap().into(),
                        memory_limit: 8 << 20,
                        log_directory: None,
                    },
                    0,
                    127,
                    Some(&owner),
                )
                .unwrap(),
            )
        } else {
            Box::new(OrderedMemoryKeyedState::new(0, 127, owner.sibling("state")).unwrap())
        };
        let mut plan = proto::NativePlan::decode(super::super::tests::plan().as_slice()).unwrap();
        let Some(proto::operator::Operator::TopN(top_n)) =
            plan.root.as_mut().and_then(|root| root.operator.as_mut())
        else {
            panic!("TopN")
        };
        top_n.rank_end = Some(64);
        let mut processor = TopNProcessor::with_state_with_range(
            &plan.encode_to_vec(),
            128,
            0,
            127,
            Box::new(Observed {
                inner: state,
                io: io.clone(),
            }),
            owner.sibling("processor"),
        )
        .unwrap();
        let values = (0..128)
            .map(|row| format!("{row:04}{}", "x".repeat(4096)))
            .collect::<Vec<_>>();
        let input = super::super::tests::batch(
            vec![1; values.len()],
            values.iter().map(String::as_str).collect(),
        );
        let error = processor.process_arrow(input, 0).unwrap_err();
        assert!(
            error.to_string().contains("append Top-N output selection"),
            "{error}"
        );
        assert_eq!(io.write_batches.load(AtomicOrdering::Relaxed), 0);
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}
