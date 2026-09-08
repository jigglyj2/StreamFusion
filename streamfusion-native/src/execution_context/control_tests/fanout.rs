// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use futures::FutureExt;

#[test]
fn shared_control_output_executes_each_stage_and_counts_its_metrics_once() {
    for event in [
        ControlEvent::Watermark(99),
        ControlEvent::BeforeCheckpoint(7),
        ControlEvent::EndInput,
    ] {
        let baseline = Fixture::new(true);
        let expected = baseline.drain(&[(2, event), (4, event)]).unwrap();
        let metrics = baseline.context.metric_snapshot().unwrap();
        let f = Fixture::new(true);
        let mut readers = f
            .context
            .start_control(f.inputs(), &[(2, event), (4, event)])
            .unwrap()
            .fan_out(2)
            .unwrap();
        let mut outputs = [Vec::new(), Vec::new()];
        let mut ended = [false; 2];
        while !ended.iter().all(|ended| *ended) {
            for index in 0..2 {
                if ended[index] {
                    continue;
                }
                match readers[index].next().now_or_never() {
                    Some(Some(Ok(batch))) if batch.num_rows() > 0 => outputs[index].push(batch),
                    Some(None) => ended[index] = true,
                    Some(Some(Err(error))) => panic!("{error}"),
                    _ => {}
                }
            }
        }
        for output in &outputs {
            assert_eq!(output, &expected);
        }
        for (left, right) in outputs[0].iter().zip(&outputs[1]) {
            for (a, b) in left.columns().iter().zip(right.columns()) {
                assert!(Arc::ptr_eq(a, b));
            }
        }
        assert_eq!(f.context.metric_snapshot().unwrap(), metrics);
        assert!(f
            .kernels
            .iter()
            .all(|kernel| kernel.lock().unwrap().controls == 3));
        let broker = f.broker.clone();
        drop(readers);
        drop(outputs);
        drop(f);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn cancellation_or_failure_poison_the_shared_persistent_invocation() {
    for fail in [false, true] {
        let f = Fixture::new(true);
        f.kernels[0].lock().unwrap().fail = fail;
        let mut readers = f
            .context
            .start_control(f.inputs(), &[(2, ControlEvent::EndInput)])
            .unwrap()
            .fan_out(2)
            .unwrap();
        if fail {
            loop {
                if f.context
                    .runtime()
                    .block_on(readers[0].next())
                    .unwrap()
                    .is_err()
                {
                    break;
                }
                // Release the other consumer before the producer can advance again.
                f.context
                    .runtime()
                    .block_on(readers[1].next())
                    .unwrap()
                    .unwrap();
            }
        } else {
            drop(readers.remove(1));
        }
        assert!(f.context.start(f.inputs()).is_err());
        assert!(f.context.snapshot_state(2, 0).is_err());
        let broker = f.broker.clone();
        drop(readers);
        drop(f);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn producer_panic_poisoning_reaches_other_consumers_and_releases_credits() {
    let f = Fixture::new(true);
    f.kernels[0].lock().unwrap().panic = true;
    let mut readers = f
        .context
        .start_control(f.inputs(), &[(2, ControlEvent::EndInput)])
        .unwrap()
        .fan_out(2)
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for _ in 0..10 {
            for reader in &mut readers {
                if let Some(Some(value)) = reader.next().now_or_never() {
                    value.unwrap();
                }
            }
        }
    }));
    assert!(panic.is_err());
    assert!(readers[1]
        .next()
        .now_or_never()
        .unwrap()
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("producer panicked"));
    assert!(f.context.start(f.inputs()).is_err());
    let broker = f.broker.clone();
    drop(readers);
    drop(f);
    assert_eq!(broker.reserved(), 0);
}
