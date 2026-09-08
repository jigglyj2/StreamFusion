// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use futures::FutureExt;

#[test]
fn one_execution_holds_the_invocation_until_every_shared_consumer_finishes() {
    let (context, broker) = context();
    let (opens, dropped) = defer(&context, batch(0), false);
    let mut readers = context.start(vec![batch(7)]).unwrap().fan_out(2).unwrap();
    let first = context
        .runtime()
        .block_on(readers[0].next())
        .unwrap()
        .unwrap();
    assert!(readers[0].next().now_or_never().is_none());
    let second = context
        .runtime()
        .block_on(readers[1].next())
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(first.column(0), second.column(0)));
    assert!(context.runtime().block_on(readers[0].next()).is_none());
    assert!(dropped.load(Ordering::Relaxed));
    assert!(context.require_idle().is_err());
    assert!(context.start(vec![batch(99)]).is_err());
    assert!(context.runtime().block_on(readers[1].next()).is_none());
    assert_eq!(opens.load(Ordering::Relaxed), 1);
    context.require_idle().unwrap();
    let mut next = context.start(vec![batch(8)]).unwrap();
    drop(readers);
    assert!(context.require_idle().is_err());
    assert_eq!(
        context.runtime().block_on(next.next()).unwrap().unwrap(),
        batch(8)
    );
    assert!(context.runtime().block_on(next.next()).is_none());
    drop(next);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn old_terminal_stream_cannot_share_or_finish_a_new_invocation() {
    let (context, broker) = context();
    let mut old = context.start(vec![batch(1)]).unwrap();
    while let Some(value) = context.runtime().block_on(old.next()) {
        value.unwrap();
    }
    let mut current = context.start(vec![batch(2)]).unwrap();
    assert!(old.fan_out(2).is_err());
    assert!(context.require_idle().is_err());
    assert_eq!(
        context.runtime().block_on(current.next()).unwrap().unwrap(),
        batch(2)
    );
    assert!(context.runtime().block_on(current.next()).is_none());
    drop(current);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn cancelled_shared_stateless_work_releases_inputs_before_a_retry() {
    let (context, broker) = context();
    let (_, dropped) = defer(&context, batch(0), false);
    let mut readers = context.start(vec![batch(3)]).unwrap().fan_out(2).unwrap();
    let held = context
        .runtime()
        .block_on(readers[0].next())
        .unwrap()
        .unwrap();
    drop(readers.remove(1));
    assert!(dropped.load(Ordering::Relaxed));
    context.require_idle().unwrap();
    let mut retry = context.start(vec![batch(4)]).unwrap();
    drop(readers);
    assert!(context.require_idle().is_err());
    assert_eq!(
        context.runtime().block_on(retry.next()).unwrap().unwrap(),
        batch(4)
    );
    assert!(context.runtime().block_on(retry.next()).is_none());
    assert_eq!(held, batch(3));
    drop(retry);
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
