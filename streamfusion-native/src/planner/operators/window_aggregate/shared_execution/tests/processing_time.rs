// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn event_time_windows_reject_processing_time_before_mutation_and_keep_legacy_capabilities() {
    let directory = tempfile::tempdir().unwrap();
    for rocks in [false, true] {
        let broker = Arc::new(TestBroker::new(256 << 20));
        let context = context(broker.clone(), None, rocks.then_some(directory.path()));
        let (bytes, memory) = context.control_capabilities().unwrap();
        let capabilities = proto::NativeControlCapabilities::decode(bytes.as_slice()).unwrap();
        assert_eq!(capabilities.protocol_version, 1);
        assert_eq!(capabilities.stages.len(), 1);
        assert!(capabilities.stages[0].watermark);
        assert!(!capabilities.stages[0].processing_time);
        drop(memory);
        let before = broker.reserved();
        let metrics = context.metric_snapshot().unwrap();
        let error = run(
            &context,
            input(&[], INSERT),
            Some(ControlEvent::ProcessingTime(9999)),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("no migrated binding"));
        context.require_idle().unwrap();
        assert_eq!(broker.reserved(), before);
        assert_eq!(context.metric_snapshot().unwrap(), metrics);
        run(&context, input(&[(7, 1, 2000)], INSERT), None).unwrap();
        assert!(
            rows(
                &run(
                    &context,
                    input(&[], INSERT),
                    Some(ControlEvent::Watermark(i64::MAX))
                )
                .unwrap()
            ) > 0
        );
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}
