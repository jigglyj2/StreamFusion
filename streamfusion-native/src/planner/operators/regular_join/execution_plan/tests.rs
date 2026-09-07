// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::super::tests::{batch, plan};
use super::*;
use crate::memory_pool::{
    tests_support::TestBroker, HostMemoryReservation, MemoryReservationBroker,
};
use crate::planner::operators::{
    arrow_handoff_tests::Observe, calc, identified::IdentifiedExec,
    reusable_input::ReusableInputExec,
};
use crate::proto;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Broker {
    inner: TestBroker,
    transfers: AtomicUsize,
}

#[test]
fn owned_input_metadata_is_not_join_payload_and_normalization_shares_arrays() {
    use crate::planner::operators::envelope::{INPUT_ROW, OWNED_TIMESTAMP_V1};
    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    let input = batch(&[1, 2], &["left", "right"], &[0, 3]);
    let owned = RecordBatch::try_from_iter(vec![
        ("key", input.column(0).clone()),
        ("value", input.column(1).clone()),
        (
            OWNED_TIMESTAMP_V1,
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])) as ArrayRef,
        ),
        ("__streamfusion_row_kind", input.column(2).clone()),
        (
            INPUT_ROW,
            Arc::new(Int32Array::from(vec![-1; 2])) as ArrayRef,
        ),
    ])
    .unwrap();
    let broker = Arc::new(TestBroker::new(1 << 20));
    let mut memory = HostMemoryReservation::new(broker, "owned join input");
    let schema = super::super::region_input::schema(&input.project(&[0, 1]).unwrap().schema());
    let expected =
        super::super::region_input::normalize(input.clone(), schema.clone(), &[0], &mut memory)
            .unwrap();
    let actual = super::super::region_input::normalize(owned, schema, &[0], &mut memory).unwrap();
    assert_eq!(actual, expected);
    for index in 0..3 {
        assert!(Arc::ptr_eq(actual.column(index), input.column(index)));
    }
}
impl MemoryReservationBroker for Broker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        self.inner.try_reserve(bytes)
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

#[tokio::test]
async fn join_to_calc_handoff_shares_arrays_and_retains_native_lease_without_arrow_java_transfer() {
    let broker = Arc::new(Broker {
        inner: TestBroker::new(16 << 20),
        transfers: AtomicUsize::new(0),
    });
    let processor = Arc::new(Mutex::new(
        RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Full),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "handoff state"),
        )
        .unwrap(),
    ));
    let raw = batch(&[1], &["left"], &[0]);
    let input_batch = RecordBatch::try_from_iter(vec![
        ("key", raw.column(0).clone()),
        ("value", raw.column(1).clone()),
        (
            crate::planner::operators::envelope::OWNED_TIMESTAMP_V1,
            Arc::new(arrow::array::Int64Array::from(vec![Some(i64::MIN)]))
                as arrow::array::ArrayRef,
        ),
        ("__streamfusion_row_kind", raw.column(2).clone()),
        (
            crate::planner::operators::envelope::INPUT_ROW,
            Arc::new(arrow::array::Int32Array::from(vec![-1])) as arrow::array::ArrayRef,
        ),
    ])
    .unwrap();
    let inputs = [0, 1].map(|_| Arc::new(ReusableInputExec::new(input_batch.schema())));
    inputs[0].replace_batch(input_batch).unwrap();
    inputs[1]
        .replace_batch(RecordBatch::new_empty(inputs[1].schema()))
        .unwrap();
    let join = IdentifiedExec::wrap(
        2,
        Arc::new(
            RegularJoinExec::new(
                processor.clone(),
                vec![
                    IdentifiedExec::wrap(3, inputs[0].clone()),
                    IdentifiedExec::wrap(4, inputs[1].clone()),
                ],
            )
            .unwrap(),
        ),
    );
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observer = Arc::new(Observe {
        input: join,
        columns: observed.clone(),
    });
    let calc = proto::Calc {
        preserve_input_envelope: false,
        input: None,
        condition: None,
        projections: (0..6)
            .map(|index| proto::Expression {
                expression: Some(proto::expression::Expression::InputReference(
                    proto::InputReference {
                        index,
                        r#type: None,
                    },
                )),
            })
            .collect(),
    };
    let physical = IdentifiedExec::wrap(1, calc::create(&calc, observer).unwrap());
    let mut stream = physical
        .execute(0, datafusion::prelude::SessionContext::new().task_ctx())
        .unwrap();
    let output = stream.next().await.unwrap().unwrap();
    for (a, b) in observed.lock().unwrap().iter().zip(output.columns()) {
        assert!(Arc::ptr_eq(a, b));
    }
    assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
    let admitted_while_consuming = broker.inner.reserved();
    let array_bytes = output.get_array_memory_size();
    assert!(admitted_while_consuming >= array_bytes);
    let retained = output.column(1).slice(0, 1);
    assert!(stream.next().await.is_none());
    assert!(broker.inner.reserved() >= array_bytes);
    assert_eq!(
        crate::plan_metrics::snapshot(&physical),
        vec![1, 1, 1, 2, 1, 1, 3, 0, 1, 4, 0, 0]
    );
    drop(stream);
    drop(output);
    observed.lock().unwrap().clear();
    drop(physical);
    drop(processor);
    assert!(
        broker.inner.reserved() > 0,
        "retained output must outlive the producer's admission"
    );
    drop(retained);
    assert_eq!(broker.inner.reserved(), 0);
}
