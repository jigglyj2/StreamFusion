// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use super::*;
use streamfusion_state_abi::RocksDbDatabaseOptions;

fn open(
    path: &Path,
    config: &RocksDbDatabaseOptions,
    scope: [u64; 2],
    checkpoint: bool,
) -> Result<RocksStateBackend> {
    RocksStateBackend::open_database_configuration(
        path,
        0,
        0,
        16 << 20,
        scope,
        None,
        checkpoint,
        0.5,
        0.1,
        config,
        &[1],
    )
}

#[test]
fn partitioned_indexes_preserve_flinks_filter_override_and_changing_restore_options() {
    for style in [0, 1, 3] {
        for bloom in [0, 1] {
            let mut filter_sizes = Vec::new();
            for bits in [0.0, 1.0, 9.9, 100.0] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("db");
                let config = RocksDbDatabaseOptions {
                    partitioned_index_filters: 1,
                    compaction_style: style,
                    bloom_enabled: bloom,
                    bloom_bits_per_key: bits,
                    bloom_block_based_mode: 1,
                    ..Default::default()
                };
                let state = open(&path, &config, [0, 0], false).unwrap();
                let options = super::database_configuration_tests::persisted(&path);
                assert_eq!(
                    options.get("index_type").map(String::as_str),
                    Some("kTwoLevelIndexSearch")
                );
                for key in [
                    "partition_filters",
                    "pin_top_level_index_and_filter",
                    "cache_index_and_filter_blocks",
                    "cache_index_and_filter_blocks_with_high_priority",
                    "pin_l0_filter_and_index_blocks_in_cache",
                ] {
                    assert_eq!(options.get(key).map(String::as_str), Some("true"), "{key}");
                }
                let expected_style = match style {
                    0 => "kCompactionStyleLevel",
                    1 => "kCompactionStyleUniversal",
                    _ => "kCompactionStyleNone",
                };
                assert_eq!(
                    options.get("compaction_style").map(String::as_str),
                    Some(expected_style)
                );
                let checkpoint = directory.path().join("checkpoint");
                filter_sizes
                    .push(super::filter_configuration_tests::contents(&state, &checkpoint).1);
                let reader = open(
                    &checkpoint,
                    &RocksDbDatabaseOptions::default(),
                    [0, 0],
                    true,
                )
                .unwrap();
                let keys = (0u32..1024)
                    .map(|index| StateKey {
                        key_group: 0,
                        key: index.to_be_bytes().to_vec(),
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    state.get_batch(&keys).unwrap(),
                    reader.get_batch(&keys).unwrap()
                );
            }
            assert!(
                filter_sizes.iter().all(|size| *size == filter_sizes[0]),
                "ten-bit override: {filter_sizes:?}"
            );
            assert_eq!(filter_sizes[0] > 0, bloom != 0);
        }
    }
}

#[test]
fn shared_resource_rejects_conflicting_partitioning_without_opening_another_database() {
    let directory = tempfile::tempdir().unwrap();
    let first_options = RocksDbDatabaseOptions {
        partitioned_index_filters: 1,
        ..Default::default()
    };
    let first = open(
        &directory.path().join("first"),
        &first_options,
        [9851, 1],
        false,
    )
    .unwrap();
    let second_options = RocksDbDatabaseOptions {
        bloom_enabled: 1,
        bloom_bits_per_key: 1.0,
        ..first_options
    };
    let second = open(
        &directory.path().join("second"),
        &second_options,
        [9851, 1],
        false,
    )
    .unwrap();
    assert!(Arc::ptr_eq(&first._shared_memory, &second._shared_memory));
    let denied = directory.path().join("denied");
    assert!(open(
        &denied,
        &RocksDbDatabaseOptions::default(),
        [9851, 1],
        false
    )
    .is_err());
    assert!(!denied.exists());
    for config in [
        RocksDbDatabaseOptions {
            partitioned_index_filters: 2,
            ..Default::default()
        },
        RocksDbDatabaseOptions {
            compaction_style: 2,
            ..Default::default()
        },
    ] {
        assert!(open(&denied, &config, [0, 0], false).is_err());
        assert!(!denied.exists());
    }
    drop(first);
    assert!(second._shared_memory.partitioned_index_filters);
}
