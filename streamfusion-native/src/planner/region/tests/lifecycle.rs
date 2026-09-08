// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;

#[test]
fn failure_panic_or_cancellation_release_outputs_and_require_recovery() {
    for mode in 0..=2 {
        let (region, task, _) = lower(&message(), 16, mode);
        let pool = task.memory_pool().clone();
        let complete = Arc::new(AtomicUsize::new(0));
        let observed = complete.clone();
        let mut output = region
            .start(
                task.clone(),
                Box::new(move |success| {
                    assert!(!success);
                    observed.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .unwrap();
        assert!(region
            .start(
                task.clone(),
                Box::new(|_| panic!("rejected invocation cannot own completion"))
            )
            .is_err());
        if mode == 0 {
            assert!(output.next().now_or_never().unwrap().unwrap().is_ok());
        } else {
            assert!(drain(&mut output, 2).is_err());
        }
        drop(output);
        assert_eq!(complete.load(Ordering::Relaxed), 1);
        assert!(region.start(task.clone(), Box::new(|_| {})).is_err());
        drop(region);
        drop(task);
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn completed_stream_cannot_finish_a_later_invocation() {
    let (region, task, _) = lower(&message(), 3, 0);
    let mut old = region
        .start(task.clone(), Box::new(|success| assert!(success)))
        .unwrap();
    drain(&mut old, 2).unwrap();
    let complete = Arc::new(AtomicUsize::new(0));
    let observed = complete.clone();
    let mut current = region
        .start(
            task.clone(),
            Box::new(move |success| {
                assert!(success);
                observed.fetch_add(1, Ordering::Relaxed);
            }),
        )
        .unwrap();
    assert!(old.next().now_or_never().unwrap().is_none());
    drop(old);
    assert_eq!(complete.load(Ordering::Relaxed), 0);
    assert!(region.start(task, Box::new(|_| {})).is_err());
    drain(&mut current, 2).unwrap();
    assert_eq!(complete.load(Ordering::Relaxed), 1);
}

#[test]
fn all_exits_stopping_early_cannot_claim_successful_shared_completion() {
    use crate::planner::persistent::PersistentOperatorFactory;
    struct Stop;
    impl PersistentOperatorFactory for Stop {
        fn supports_owned_envelope(&self) -> bool {
            true
        }
        fn build(
            &self,
            _: &proto::Operator,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                children.remove(0).schema(),
            )))
        }
    }
    let mut plan = message();
    plan.stages[2].inputs[0] = plan.stages[1].inputs[0].clone();
    plan.output_stage_ids = vec![4294967302, 4294967303];
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let contract = RegionPlan::decode(&plan.encode_to_vec(), &pool).unwrap();
    let (input, _) = source(100, 0);
    let region = PhysicalRegion::lower(
        contract,
        vec![input],
        &[(4294967302, Arc::new(Stop)), (4294967303, Arc::new(Stop))],
        pool.clone(),
    )
    .unwrap();
    let mut stream = region
        .start(task(pool), Box::new(|success| assert!(!success)))
        .unwrap();
    let error = drain(&mut stream, 2).unwrap_err();
    assert!(error
        .to_string()
        .contains("before their shared producers drained"));
}

#[test]
fn construction_failure_cleans_up_the_armed_shared_owner() {
    use crate::planner::operators::reusable_input::ReusableInputExec;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let contract = RegionPlan::decode(&message().encode_to_vec(), &pool).unwrap();
    // Negotiated input schema with no installed invocation batch fails during execute.
    let input = Arc::new(ReusableInputExec::new(batch(0).schema()));
    let region = PhysicalRegion::lower(contract, vec![input], &[], pool.clone()).unwrap();
    let complete = Arc::new(AtomicUsize::new(0));
    let observed = complete.clone();
    assert!(region
        .start(
            task(pool.clone()),
            Box::new(move |success| {
                assert!(!success);
                observed.fetch_add(1, Ordering::Relaxed);
            })
        )
        .is_err());
    assert_eq!(complete.load(Ordering::Relaxed), 1);
    assert!(region.start(task(pool.clone()), Box::new(|_| {})).is_err());
    drop(region);
    assert_eq!(pool.reserved(), 0);
}
