// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::io::{Cursor, Read};

struct Input {
    bytes: Cursor<Vec<u8>>,
    largest_read: usize,
}
impl Read for Input {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.largest_read = self.largest_read.max(bytes.len());
        self.bytes.read(bytes)
    }
}

#[test]
fn canonical_stream_restores_wide_state_without_a_whole_frame_reservation_on_either_backend() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    for plan in plans() {
        for rocks in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let limit = if rocks { 4 << 20 } else { 20 << 20 };
            let broker = Arc::new(TestBroker::new(limit));
            let mut context = NativeExecutionContext::new(
                &plan.encode_to_vec(),
                Arc::new(FlinkMemoryPool::new(broker.clone(), limit)),
            )
            .unwrap();
            let mut options = resources();
            options.protocol_version = 4;
            options.spill_directories = vec![directory.path().to_str().unwrap().to_owned()];
            if rocks {
                options.bindings[0].backend = Some(proto::native_state_binding::Backend::Rocksdb(
                    proto::NativeRocksDbState {
                        plugin_path: plugin.clone(),
                        database_path: directory.path().join("db").to_str().unwrap().into(),
                        memory_limit: 1 << 20,
                        log_directory: None,
                    },
                ));
            }
            install(&mut context, &broker, &options.encode_to_vec()).unwrap();
            let entries = (0u32..3201)
                .rev()
                .map(|id| (id.to_be_bytes().to_vec(), vec![(id % 251) as u8; 4096]))
                .collect::<Vec<_>>();
            let bytes = streamfusion_state_abi::encode_key_group_snapshot(
                7,
                entries
                    .iter()
                    .map(|(key, value)| (key.as_slice(), value.as_slice())),
            )
            .unwrap();
            let length = bytes.len();
            assert!(length > 12 << 20);
            let mut input = Input {
                bytes: Cursor::new(bytes),
                largest_read: 0,
            };
            context
                .restore_state_reader(2, 7, length as u64, &mut input)
                .unwrap();
            assert_eq!(input.bytes.position(), length as u64);
            assert!(input.largest_read <= 64 << 10);
            let expected = streamfusion_state_abi::encode_key_group_snapshot(
                7,
                entries
                    .iter()
                    .rev()
                    .map(|(key, value)| (key.as_slice(), value.as_slice())),
            )
            .unwrap();
            let mut actual = Vec::new();
            let written = context
                .write_snapshot_state(2, 7, &mut |bytes| {
                    actual.extend_from_slice(bytes);
                    Ok(())
                })
                .unwrap();
            assert_eq!(written, actual.len());
            assert_eq!(&actual[4..], expected);
            drop(context);
            assert_eq!(broker.reserved(), 0);
        }
    }
}

#[test]
fn corrupt_and_truncated_large_frames_leave_the_region_idle_and_release_preparation() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let mut context = context(&plans().remove(0), &broker);
    let mut options = resources();
    options.protocol_version = 4;
    options.spill_directories = vec![directory.path().to_str().unwrap().to_owned()];
    install(&mut context, &broker, &options.encode_to_vec()).unwrap();
    let baseline = broker.reserved();
    let empty = context.snapshot_state(2, 7).unwrap().to_vec();
    let duplicate = streamfusion_state_abi::encode_key_group_snapshot(
        7,
        [
            (b"key".as_slice(), b"one".as_slice()),
            (b"key".as_slice(), b"two".as_slice()),
        ]
        .into_iter(),
    )
    .unwrap();
    for (length, bytes) in [
        (5u64 << 30, empty.clone()),
        (duplicate.len() as u64, duplicate),
    ] {
        let error = context
            .restore_state_reader(2, 7, length, &mut Cursor::new(bytes))
            .unwrap_err();
        assert!(
            !matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        context.require_idle().unwrap();
        assert_eq!(broker.reserved(), baseline);
        assert_eq!(context.snapshot_state(2, 7).unwrap().as_ref(), empty);
    }
    drop(context);
    assert_eq!(broker.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
