// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use arrow::array::StringArray;
use futures::StreamExt;
use std::path::Path;

fn configured(plugin: &str, path: &Path, codes: Vec<u32>) -> proto::NativeStateBindings {
    let mut bindings = resources();
    bindings.protocol_version = 11;
    bindings.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
        proto::NativeRocksDbState {
            plugin_path: plugin.into(),
            database_path: path.to_str().unwrap().into(),
            memory_limit: 4 << 20,
            statistics_tickers: codes,
            ..Default::default()
        },
    ));
    bindings
}

#[test]
fn malformed_statistics_bindings_fail_before_loading_or_allocating_state() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(LIMIT));
    let mut context = context(&plans()[0], &broker);
    let baseline = broker.reserved();
    for (version, codes, reason) in [
        (10, vec![7], "protocol 11"),
        (11, vec![7, 7], "duplicate"),
        (11, vec![u32::MAX], "unsupported"),
        (11, vec![0; 12], "eleven"),
    ] {
        let mut options = configured("/missing.so", &directory.path().join("db"), codes);
        options.protocol_version = version;
        let error = install(&mut context, &broker, &options.encode_to_vec()).unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
        assert_eq!(broker.reserved(), baseline);
        assert!(context.persistent.is_empty());
        assert!(context.state_resources.is_none());
        assert!(!directory.path().join("db").exists());
    }
}

#[test]
fn generated_statistics_follow_context_execution_checkpoint_import_and_cleanup() {
    let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
        return;
    };
    for plan in plans() {
        let root = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(LIMIT));
        let source = lifecycle::bound(
            &plan,
            &configured(&plugin, &root.path().join("source"), vec![7, 6, 5]),
            &broker,
        );
        let destination = lifecycle::bound(
            &plan,
            &configured(&plugin, &root.path().join("destination"), vec![6, 7]),
            &broker,
        );
        let oracle = lifecycle::bound(&plan, &resources(), &broker);
        let (schema, memory) = source.state_statistics_schema().unwrap();
        let schema = proto::NativeGaugeSchema::decode(schema.as_slice()).unwrap();
        drop(memory);
        assert_eq!(schema.protocol_version, 1);
        let expected = [
            "rocksdb.bytes_written",
            "rocksdb.iter_bytes_read",
            "rocksdb.bytes_read",
        ];
        for (gauge, name) in schema.gauges.iter().zip(expected) {
            assert_eq!(gauge.plan_node_id, 2);
            assert_eq!(gauge.name, name);
            assert!(gauge.groups.is_empty());
            assert_eq!(gauge.value_kind, proto::NativeGaugeValueKind::Int64 as i32);
            assert_eq!(gauge.metric_kind, proto::NativeMetricKind::Gauge as i32);
        }
        assert_eq!(schema.gauges.len(), 3);
        assert!(oracle.state_statistics_snapshot().unwrap().0.is_empty());
        assert_eq!(source.state_statistics_snapshot().unwrap().0, vec![0, 0, 0]);
        let keys = (0..617)
            .map(|index| (index % 17 != 0).then(|| format!("key-{}", index % 257)))
            .collect::<Vec<_>>();
        let input = lifecycle::input(Arc::new(StringArray::from(keys)), 64);
        let expected = lifecycle::drain(&oracle, &input);
        let mut stream = source.start(vec![input.clone()]).unwrap();
        assert!(source.require_idle().is_err());
        // Sampling must work while an invocation is active and from a separate Flink view thread.
        let sampler_context = Arc::clone(&source);
        let sampler = std::thread::spawn(move || {
            let mut previous = 0;
            for _ in 0..100 {
                let (values, _memory) = sampler_context.state_statistics_snapshot().unwrap();
                assert!(values[0] >= previous);
                previous = values[0];
            }
        });
        let mut actual = Vec::new();
        while let Some(batch) = source.runtime().block_on(stream.next()) {
            actual.push(batch.unwrap());
        }
        sampler.join().unwrap();
        assert_eq!(actual, expected);
        let (values, memory) = source.state_statistics_snapshot().unwrap();
        assert!(values[0] > 0);
        assert_eq!(values[2], 0, "MultiGet must not fabricate BYTES_READ");
        drop(memory);
        let checkpoint = root.path().join("checkpoint");
        source.checkpoint_state(2, &checkpoint).unwrap();
        let source_reads = source.state_statistics_snapshot().unwrap().0[1];
        destination
            .import_state_checkpoint(2, Path::new(&plugin), &checkpoint, 0, 15, 4 << 20)
            .unwrap();
        let (values, memory) = destination.state_statistics_snapshot().unwrap();
        assert!(
            values[0] > 0,
            "temporary reader scans must reach destination statistics"
        );
        assert!(
            values[1] > 0,
            "restored writes must reach destination statistics"
        );
        drop(memory);
        assert_eq!(
            source.state_statistics_snapshot().unwrap().0[1],
            source_reads
        );
        for group in 0..16 {
            assert_eq!(
                source.snapshot_state(2, group).unwrap(),
                destination.snapshot_state(2, group).unwrap()
            );
        }
        assert_eq!(
            lifecycle::drain(&destination, &input),
            lifecycle::drain(&oracle, &input)
        );
        drop(stream);
        drop(actual);
        drop(expected);
        drop(input);
        drop(source);
        drop(destination);
        drop(oracle);
        assert_eq!(broker.reserved(), 0);
    }
}
