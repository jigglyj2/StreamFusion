// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::abi::{backend, API};
use crate::{StateKey, StateMutation};
use streamfusion_state_abi::{RocksDbDatabaseOptions, StateBackendOpenOptions, STATE_BACKEND_OK};

struct Reader(*mut c_void);
impl Reader {
    fn new(db: *mut c_void) -> Self {
        let mut handle = ptr::null_mut();
        assert_eq!(
            unsafe { (API.open_statistics)(db, &mut handle) },
            STATE_BACKEND_OK
        );
        assert!(!handle.is_null());
        Self(handle)
    }
    fn sample(&self) -> [u64; 11] {
        let mut values = [0; 11];
        let codes = std::array::from_fn::<_, 11, _>(|index| index as u32);
        assert_eq!(
            unsafe { (API.read_statistics)(self.0, codes.as_ptr(), 11, values.as_mut_ptr()) },
            STATE_BACKEND_OK
        );
        values
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        unsafe { (API.close_statistics)(self.0) };
    }
}

#[derive(Debug)]
struct Database(*mut c_void);
impl Database {
    fn open(
        path: &std::path::Path,
        enabled: u8,
        owner: Option<&Reader>,
        checkpoint: bool,
    ) -> Result<Self, String> {
        let path = path.to_str().unwrap();
        let options = StateBackendOpenOptions {
            struct_size: std::mem::size_of::<StateBackendOpenOptions>(),
            path: path.as_ptr(),
            path_len: path.len(),
            first_key_group: 0,
            last_key_group: 1,
            memory_limit: 4 << 20,
            memory_scope_high: 0,
            memory_scope_low: 0,
            log_directory: ptr::null(),
            log_directory_len: 0,
            write_buffer_ratio: 0.5,
            high_priority_pool_ratio: 0.1,
            database_options: RocksDbDatabaseOptions {
                statistics_enabled: enabled,
                ..Default::default()
            },
            compression_per_level: ptr::null(),
            compression_per_level_len: 0,
            statistics_owner: owner.map_or(ptr::null(), |r| r.0),
        };
        let mut handle = ptr::dangling_mut();
        let status = unsafe {
            (if checkpoint {
                API.open_checkpoint
            } else {
                API.open
            })(&options, &mut handle)
        };
        if status != STATE_BACKEND_OK {
            assert!(handle.is_null());
            return Err(unsafe { std::ffi::CStr::from_ptr((API.last_error)()) }
                .to_string_lossy()
                .into_owned());
        }
        Ok(Self(handle))
    }
    fn state(&self) -> &crate::RocksStateBackend {
        backend(self.0).unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        unsafe { (API.close)(self.0) };
    }
}

fn rows() -> Vec<StateMutation> {
    (0..617u32)
        .map(|index| StateMutation {
            key: StateKey {
                key_group: index % 2,
                key: index.to_be_bytes().to_vec(),
            },
            value: (index % 7 != 0).then(|| vec![index as u8; (index % 251 + 1) as usize]),
        })
        .collect()
}

#[test]
fn names_match_the_actual_flink_monitor_fixture() {
    assert_eq!(
        ROCKSDB_TICKER_NAMES.join("\n"),
        include_str!("../../../tests/fixtures/flink-rocksdb-ticker-names.txt").trim()
    );
}

#[test]
fn statistics_preserve_generated_state_and_sample_all_tickers_after_database_close() {
    let root = tempfile::tempdir().unwrap();
    let plain = Database::open(&root.path().join("plain"), 0, None, false).unwrap();
    let measured = Database::open(&root.path().join("measured"), 1, None, false).unwrap();
    let reader = Reader::new(measured.0);
    assert_eq!(reader.sample()[7], 0);
    for db in [&plain, &measured] {
        db.state().write_batch(rows()).unwrap();
    }
    assert!(reader.sample()[7] > 0);
    for group in 0..2 {
        assert_eq!(
            plain.state().snapshot_key_group(group).unwrap(),
            measured.state().snapshot_key_group(group).unwrap()
        );
    }
    let keys = rows().into_iter().map(|row| row.key).collect::<Vec<_>>();
    assert_eq!(
        plain.state().get_batch(&keys).unwrap(),
        measured.state().get_batch(&keys).unwrap()
    );
    let sample = reader.sample();
    // Upstream MultiGet counts NUMBER_MULTIGET_BYTES_READ, not BYTES_READ.
    assert_eq!(sample[5], 0);
    assert!(sample[6] > 0);
    let options = measured.state().statistics_reader().unwrap();
    for code in 0..11 {
        assert_eq!(sample[code], options.get_ticker_count(ticker(code as u32)));
    }
    drop(measured);
    let closed = reader.sample();
    assert_eq!(reader.sample(), closed);
    assert!(closed[7] >= sample[7]);
}

#[test]
fn checkpoint_reads_share_destination_statistics_without_source_counters() {
    let root = tempfile::tempdir().unwrap();
    let source = Database::open(&root.path().join("source"), 1, None, false).unwrap();
    source.state().write_batch(rows()).unwrap();
    let source_stats = Reader::new(source.0);
    let checkpoint = root.path().join("checkpoint");
    source.state().checkpoint(&checkpoint).unwrap();
    let destination = Database::open(&root.path().join("destination"), 1, None, false).unwrap();
    let destination_stats = Reader::new(destination.0);
    let restored = Database::open(&checkpoint, 1, Some(&destination_stats), true).unwrap();
    let restored_stats = Reader::new(restored.0);
    // Neither the original reader wrapper nor the original DB is needed by the shared reader.
    drop(destination_stats);
    drop(destination);
    let before = restored_stats.sample();
    let source_before = source_stats.sample()[6];
    let keys = rows().into_iter().map(|row| row.key).collect::<Vec<_>>();
    assert_eq!(
        restored.state().get_batch(&keys).unwrap(),
        rows().into_iter().map(|row| row.value).collect::<Vec<_>>()
    );
    restored.state().snapshot_key_group(0).unwrap();
    let after = restored_stats.sample();
    assert!(after[6] > before[6]);
    assert_eq!(
        after[7], 0,
        "destination statistics must not inherit source writes"
    );
    assert_eq!(source_stats.sample()[6], source_before);
    let other_reader = Reader::new(restored.0);
    assert_eq!(other_reader.sample(), after);
    drop(restored);
    assert_eq!(restored_stats.sample(), other_reader.sample());
}

#[test]
fn rejects_disabled_statistics_invalid_flags_and_malformed_requests_transactionally() {
    let root = tempfile::tempdir().unwrap();
    let disabled = Database::open(&root.path().join("disabled"), 0, None, false).unwrap();
    let mut result = ptr::dangling_mut();
    assert_ne!(
        unsafe { (API.open_statistics)(disabled.0, &mut result) },
        STATE_BACKEND_OK
    );
    assert!(result.is_null());
    assert!(Database::open(&root.path().join("invalid"), 2, None, false).is_err());
    assert!(!root.path().join("invalid").exists());
    let enabled = Database::open(&root.path().join("enabled"), 1, None, false).unwrap();
    let reader = Reader::new(enabled.0);
    assert!(
        Database::open(&root.path().join("inconsistent"), 0, Some(&reader), false)
            .unwrap_err()
            .contains("require statistics")
    );
    assert!(!root.path().join("inconsistent").exists());
    for codes in [vec![0, 11], vec![0, u32::MAX], vec![0, 0], vec![0; 12]] {
        let mut values = [u64::MAX; 12];
        assert_ne!(
            unsafe {
                (API.read_statistics)(reader.0, codes.as_ptr(), codes.len(), values.as_mut_ptr())
            },
            STATE_BACKEND_OK
        );
        assert_eq!(values, [u64::MAX; 12]);
    }
    unsafe {
        assert_ne!(
            (API.open_statistics)(ptr::null_mut(), &mut result),
            STATE_BACKEND_OK
        );
        assert!(result.is_null());
        assert_ne!(
            (API.open_statistics)(enabled.0, ptr::null_mut()),
            STATE_BACKEND_OK
        );
        assert_ne!(
            (API.read_statistics)(ptr::null(), ptr::null(), 0, ptr::null_mut()),
            STATE_BACKEND_OK
        );
        assert_eq!(
            (API.read_statistics)(reader.0, ptr::null(), 0, ptr::null_mut()),
            STATE_BACKEND_OK
        );
        assert_ne!(
            (API.read_statistics)(reader.0, ptr::null(), 1, &mut 0),
            STATE_BACKEND_OK
        );
        assert_ne!(
            (API.read_statistics)(reader.0, &0, 1, ptr::null_mut()),
            STATE_BACKEND_OK
        );
        (API.close_statistics)(ptr::null_mut());
    }
}

#[test]
fn statistics_sampling_can_run_concurrently_with_batched_writes() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open(root.path(), 1, None, false).unwrap();
    let reader = Reader::new(db.0);
    std::thread::scope(|scope| {
        let state = db.state();
        let writer = scope.spawn(move || {
            for _ in 0..20 {
                state.write_batch(rows()).unwrap();
            }
        });
        let mut previous = 0;
        for _ in 0..1000 {
            let current = reader.sample()[7];
            assert!(current >= previous);
            previous = current;
        }
        writer.join().unwrap();
        assert!(reader.sample()[7] > 0);
    });
}
