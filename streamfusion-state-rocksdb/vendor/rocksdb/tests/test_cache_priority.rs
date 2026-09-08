// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
#![cfg(feature = "cache-priority")]

use rocksdb::{BlockBasedOptions, Cache, LruCacheOptions, Options, DB};

#[test]
fn configured_priority_reaches_the_opened_database_cache() {
    let directory = tempfile::tempdir().unwrap();
    let mut cache_options = LruCacheOptions::default();
    cache_options.set_capacity(8 << 20);
    cache_options.set_high_pri_pool_ratio(0.1);
    let cache = Cache::new_lru_cache_opts(&cache_options);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(&cache);
    table.set_cache_index_and_filter_blocks(true);
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_block_based_table_factory(&table);
    {
        let db = DB::open(&options, directory.path()).unwrap();
        db.put(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap().unwrap(), b"value");
        assert_eq!(
            db.property_int_value("rocksdb.block-cache-capacity")
                .unwrap(),
            Some(8 << 20)
        );
    }
    let log = std::fs::read_to_string(directory.path().join("LOG")).unwrap();
    assert!(log.contains("high_pri_pool_ratio: 0.100"), "{log}");
}

#[test]
fn invalid_ratios_are_rejected_before_calling_rocksdb() {
    for ratio in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        assert!(std::panic::catch_unwind(
            || LruCacheOptions::default().set_high_pri_pool_ratio(ratio)
        )
        .is_err());
    }
}
