// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use streamfusion_state_abi::RocksDbDatabaseOptions;

pub(super) fn open(
    path: &Path,
    config: &RocksDbDatabaseOptions,
    compression: &[i32],
    checkpoint: bool,
) -> RocksStateBackend {
    RocksStateBackend::open_database_configuration(
        path,
        0,
        0,
        16 << 20,
        [0, 0],
        None,
        checkpoint,
        0.5,
        0.1,
        config,
        compression,
    )
    .unwrap()
}

pub(super) fn contents(state: &RocksStateBackend, checkpoint: &Path) -> (u64, u64) {
    let value = b"compressible-state".repeat(512);
    let entries = (0u32..512)
        .map(|index| StateMutation {
            key: StateKey {
                key_group: 0,
                key: index.to_be_bytes().to_vec(),
            },
            value: Some(value.clone()),
        })
        .collect();
    state.write_batch(entries).unwrap();
    let files = state.checkpoint(checkpoint).unwrap();
    let sst_bytes = files
        .files
        .iter()
        .filter(|file| {
            file.relative_path
                .extension()
                .is_some_and(|ext| ext == "sst")
        })
        .map(|file| file.size)
        .sum();
    assert_eq!(
        state
            .db
            .property_int_value("rocksdb.block-cache-capacity")
            .unwrap(),
        Some((2.5 * (16 << 20) as f64 / 3.0) as u64)
    );
    let properties = state
        .db
        .property_value("rocksdb.aggregated-table-properties")
        .unwrap()
        .unwrap();
    let filter_size = properties
        .split(';')
        .find_map(|property| {
            let (name, value) = property.trim().split_once('=')?;
            (name.trim() == "filter block size").then(|| value.trim().parse::<u64>().unwrap())
        })
        .unwrap_or_else(|| panic!("missing filter size: {properties}"));
    (sst_bytes, filter_size)
}

#[test]
fn actual_ssts_use_selected_compression_and_restore_with_different_filter_options() {
    let mut sizes = Vec::new();
    for compression in [vec![0], vec![1], vec![0, 1, 0], vec![]] {
        let directory = tempfile::tempdir().unwrap();
        let config = RocksDbDatabaseOptions {
            bloom_enabled: 1,
            bloom_bits_per_key: 9.9,
            ..Default::default()
        };
        let state = open(&directory.path().join("db"), &config, &compression, false);
        let persisted =
            super::database_configuration_tests::persisted(&directory.path().join("db"));
        let expected = compression
            .iter()
            .map(|value| {
                if *value == 0 {
                    "kNoCompression"
                } else {
                    "kSnappyCompression"
                }
            })
            .collect::<Vec<_>>()
            .join(":");
        assert_eq!(
            persisted
                .get("compression_per_level")
                .map(String::as_str)
                .unwrap_or(""),
            expected
        );
        let checkpoint = directory.path().join("checkpoint");
        let (bytes, filter_size) = contents(&state, &checkpoint);
        assert!(filter_size > 0);
        sizes.push(bytes);
        let reader = open(&checkpoint, &RocksDbDatabaseOptions::default(), &[1], true);
        let keys = (0u32..1024)
            .map(|index| StateKey {
                key_group: 0,
                key: index.to_be_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            reader.get_batch(&keys).unwrap(),
            state.get_batch(&keys).unwrap()
        );
    }
    assert!(
        sizes[0] > sizes[1] * 4,
        "actual compression sizes: {sizes:?}"
    );
    assert_eq!(
        sizes[0], sizes[2],
        "L0 must use the first compression entry"
    );
    assert_eq!(
        sizes[1], sizes[3],
        "an empty list retains base Snappy compression"
    );
}

#[test]
fn bloom_filter_sizes_follow_flinks_rounding_and_obsolete_mode_flag() {
    let mut observed = Vec::new();
    for bits in [
        -1.0,
        0.0,
        0.49,
        0.5,
        1.0,
        9.9,
        100.0,
        f64::INFINITY,
        f64::NAN,
    ] {
        let mut modes = Vec::new();
        for mode in [0, 1] {
            let directory = tempfile::tempdir().unwrap();
            let config = RocksDbDatabaseOptions {
                bloom_enabled: 1,
                bloom_bits_per_key: bits,
                bloom_block_based_mode: mode,
                ..Default::default()
            };
            let state = open(&directory.path().join("db"), &config, &[1], false);
            modes.push(contents(&state, &directory.path().join("checkpoint")).1);
        }
        assert_eq!(modes[0], modes[1], "obsolete flag, bits={bits}");
        observed.push(modes[0]);
    }
    assert_eq!(&observed[..3], &[0, 0, 0]);
    assert_eq!(observed[3], observed[4]);
    assert!(observed[4] > 0 && observed[5] > observed[4] && observed[6] > observed[5]);
    assert_eq!(observed[6], observed[7]);
    assert_eq!(observed[6], observed[8]);
}

#[test]
fn all_supported_log_levels_reach_the_opened_database() {
    for (level, name) in [
        "DEBUG_LEVEL",
        "INFO_LEVEL",
        "WARN_LEVEL",
        "ERROR_LEVEL",
        "FATAL_LEVEL",
        "HEADER_LEVEL",
    ]
    .iter()
    .enumerate()
    {
        let directory = tempfile::tempdir().unwrap();
        let config = RocksDbDatabaseOptions {
            log_level: level as u32,
            ..Default::default()
        };
        let _state = open(directory.path(), &config, &[1], false);
        assert_eq!(
            super::database_configuration_tests::persisted(directory.path())
                .get("info_log_level")
                .map(String::as_str),
            Some(*name)
        );
    }
}
