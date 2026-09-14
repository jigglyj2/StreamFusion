// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

/// ABI 18: complete resolved Flink database/table settings. No resource ownership crosses here.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RocksDbDatabaseOptions {
    pub max_background_jobs: i32,
    pub max_open_files: i32,
    pub max_log_file_size: u64,
    pub keep_log_file_num: u64,
    pub dynamic_level_bytes: u8,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
    pub write_buffer_size: u64,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,
    pub periodic_compaction_seconds: u64,
    pub block_size: u64,
    pub metadata_block_size: u64,
    /// Stable ABI log-level codes: debug=0, info=1, warn=2, error=3, fatal=4, header=5.
    pub log_level: u32,
    pub bloom_enabled: u8,
    pub bloom_bits_per_key: f64,
    pub bloom_block_based_mode: u8,
    /// Resolved from the first owner of the shared Flink memory resource.
    pub partitioned_index_filters: u8,
    /// Level=0, universal=1, no background compaction=3. FIFO is deliberately unsupported.
    pub compaction_style: u32,
    /// Explicit Flink log directories remain user-owned; automatic relocation is cleaned.
    pub retain_log_files: u8,
    /// Flink bulk-write threshold; zero disables byte-triggered flushing, not the count limit.
    pub write_batch_size: u64,
    /// Internal resolved capability; deployment admission remains on the Java metric surface.
    pub statistics_enabled: u8,
}

impl Default for RocksDbDatabaseOptions {
    fn default() -> Self {
        Self {
            max_background_jobs: 2,
            max_open_files: -1,
            max_log_file_size: 25 << 20,
            keep_log_file_num: 4,
            dynamic_level_bytes: 0,
            target_file_size_base: 64 << 20,
            max_bytes_for_level_base: 256 << 20,
            write_buffer_size: 64 << 20,
            max_write_buffer_number: 2,
            min_write_buffer_number_to_merge: 1,
            periodic_compaction_seconds: 30 * 24 * 60 * 60,
            block_size: 4096,
            metadata_block_size: 4096,
            log_level: 1,
            bloom_enabled: 0,
            bloom_bits_per_key: 10.0,
            bloom_block_based_mode: 0,
            partitioned_index_filters: 0,
            compaction_style: 0,
            retain_log_files: 0,
            write_batch_size: 2 << 20,
            statistics_enabled: 0,
        }
    }
}

impl RocksDbDatabaseOptions {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !matches!(self.compaction_style, 0 | 1 | 3) {
            return Err("unsupported Flink RocksDB compaction style");
        }
        if self.max_background_jobs <= 0
            || self.keep_log_file_num == 0
            || self.keep_log_file_num > i32::MAX as u64
            || self.max_write_buffer_number <= 0
            || self.min_write_buffer_number_to_merge <= 0
            || self.dynamic_level_bytes > 1
            || self.log_level > 5
            || self.bloom_enabled > 1
            || self.bloom_block_based_mode > 1
            || self.partitioned_index_filters > 1
            || self.retain_log_files > 1
            || self.statistics_enabled > 1
        {
            return Err("invalid Flink RocksDB database option count or boolean");
        }
        for value in [
            self.target_file_size_base,
            self.max_bytes_for_level_base,
            self.write_buffer_size,
            self.block_size,
            self.metadata_block_size,
        ] {
            if value == 0 || value > i64::MAX as u64 || usize::try_from(value).is_err() {
                return Err("invalid Flink RocksDB database option size");
            }
        }
        if self.write_batch_size > i64::MAX as u64
            || usize::try_from(self.write_batch_size).is_err()
        {
            return Err("invalid Flink RocksDB write-batch-size");
        }
        if self.max_log_file_size > i64::MAX as u64
            || usize::try_from(self.max_log_file_size).is_err()
            || self.periodic_compaction_seconds > i64::MAX as u64
        {
            return Err("invalid Flink RocksDB log size or compaction duration");
        }
        Ok(())
    }
}

/// Codecs compiled into the optional component; codes match Flink/RocksDB CompressionType.
/// XPRESS (6), obsolete ZSTD (64), sentinels, and unknown codes are rejected.
pub fn validate_rocksdb_compression(levels: &[i32]) -> Result<(), &'static str> {
    if levels.iter().any(|level| !matches!(level, 0..=5 | 7)) {
        return Err("unsupported Flink RocksDB per-level compression codec");
    }
    Ok(())
}
