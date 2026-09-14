// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::filter_configuration_tests::{contents, open};
use super::*;
use streamfusion_state_abi::RocksDbDatabaseOptions;

#[test]
fn compiled_codecs_compress_real_ssts_and_restore_with_a_different_destination_codec() {
    let mut uncompressed_bytes = 0;
    for (codec, name) in [
        (0, "kNoCompression"),
        (1, "kSnappyCompression"),
        (2, "kZlibCompression"),
        (3, "kBZip2Compression"),
        (4, "kLZ4Compression"),
        (5, "kLZ4HCCompression"),
        (7, "kZSTD"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("db");
        let checkpoint = directory.path().join("checkpoint");
        let config = RocksDbDatabaseOptions::default();
        // Mixed lists verify L0 uses the selected codec, even when a higher level differs.
        let state = open(&source, &config, &[codec, 0, 7], false);
        let persisted = super::database_configuration_tests::persisted(&source);
        assert_eq!(
            persisted["compression_per_level"],
            format!("{name}:kNoCompression:kZSTD")
        );
        let (bytes, _) = contents(&state, &checkpoint);
        if codec == 0 {
            uncompressed_bytes = bytes;
        } else {
            assert!(
                bytes * 4 < uncompressed_bytes,
                "{name} silently left SSTs uncompressed: {bytes}/{uncompressed_bytes}"
            );
        }
        let keys = (0u32..1024)
            .map(|index| StateKey {
                key_group: 0,
                key: index.to_be_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        let expected = state.get_batch(&keys).unwrap();
        drop(state);
        // Checkpoint readers use destination settings; old SSTs retain their own codec.
        let reader = open(
            &checkpoint,
            &config,
            &[if codec == 7 { 4 } else { 7 }],
            true,
        );
        assert_eq!(reader.get_batch(&keys).unwrap(), expected, "{name}");
        drop(reader);
        let reopened = open(&source, &config, &[0], false);
        assert_eq!(
            reopened.get_batch(&keys).unwrap(),
            expected,
            "live reopen: {name}"
        );
    }
}

#[test]
fn unsupported_compression_codes_fail_before_creating_a_database() {
    for codec in [-1, 6, 8, 64, 127, i32::MAX] {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("db");
        assert!(RocksStateBackend::open_database_configuration(
            &db,
            0,
            0,
            16 << 20,
            [0, 0],
            None,
            false,
            0.5,
            0.1,
            &RocksDbDatabaseOptions::default(),
            &[codec],
        )
        .is_err());
        assert!(!db.exists(), "rejected codec {codec} mutated storage");
    }
}
