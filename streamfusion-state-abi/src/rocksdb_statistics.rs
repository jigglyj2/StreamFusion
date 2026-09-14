// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

/// ABI-local codes, in this order, independent of upstream RocksDB enum ordinals.
/// Names match Flink RocksDBNativeMetricMonitor's database-level Gauge<Long> names.
pub const ROCKSDB_TICKER_NAMES: [&str; 11] = [
    "rocksdb.block_cache_hit",
    "rocksdb.block_cache_miss",
    "rocksdb.bloom_filter_useful",
    "rocksdb.bloom_filter_full_positive",
    "rocksdb.bloom_filter_full_true_positive",
    "rocksdb.bytes_read",
    "rocksdb.iter_bytes_read",
    "rocksdb.bytes_written",
    "rocksdb.compact_read_bytes",
    "rocksdb.compact_write_bytes",
    "rocksdb.stall_micros",
];

pub fn validate_rocksdb_tickers(codes: &[u32]) -> Result<(), String> {
    if codes.len() > ROCKSDB_TICKER_NAMES.len() {
        return Err("RocksDB statistics request exceeds eleven tickers".into());
    }
    let mut seen = 0u16;
    for &code in codes {
        if code as usize >= ROCKSDB_TICKER_NAMES.len() {
            return Err(format!("unsupported RocksDB statistics ticker code {code}"));
        }
        let bit = 1u16 << code;
        if seen & bit != 0 {
            return Err(format!("duplicate RocksDB statistics ticker code {code}"));
        }
        seen |= bit;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_codes_are_bounded_and_unique() {
        assert!(validate_rocksdb_tickers(&[]).is_ok());
        assert!(validate_rocksdb_tickers(&(0..11).rev().collect::<Vec<_>>()).is_ok());
        for invalid in [vec![11], vec![u32::MAX], vec![1, 1], vec![0; 12]] {
            assert!(validate_rocksdb_tickers(&invalid).is_err());
        }
    }
}
