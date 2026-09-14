// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0
use datafusion::common::{DataFusionError, Result};
use streamfusion_state_abi::RocksDbDatabaseOptions;

pub(crate) fn resolve(
    options: Option<&crate::proto::NativeRocksDbOptions>,
) -> Result<RocksDbDatabaseOptions> {
    let Some(options) = options else {
        return Ok(RocksDbDatabaseOptions::default());
    };
    if let Some(directory) = &options.log_directory {
        if !std::path::Path::new(directory).is_absolute() || directory.contains('\0') {
            return Err(DataFusionError::Execution(
                "Explicit RocksDB log directory must be an absolute path without NUL bytes".into(),
            ));
        }
    }
    let resolved = RocksDbDatabaseOptions {
        max_background_jobs: options.max_background_jobs,
        max_open_files: options.max_open_files,
        max_log_file_size: options.max_log_file_size,
        keep_log_file_num: options.keep_log_file_num,
        dynamic_level_bytes: options.dynamic_level_bytes as u8,
        target_file_size_base: options.target_file_size_base,
        max_bytes_for_level_base: options.max_bytes_for_level_base,
        write_buffer_size: options.write_buffer_size,
        max_write_buffer_number: options.max_write_buffer_number,
        min_write_buffer_number_to_merge: options.min_write_buffer_number_to_merge,
        periodic_compaction_seconds: options.periodic_compaction_seconds,
        block_size: options.block_size,
        metadata_block_size: options.metadata_block_size,
        log_level: options.log_level.unwrap_or(1),
        compaction_style: options.compaction_style.unwrap_or(0),
        partitioned_index_filters: 0,
        retain_log_files: options.log_directory.is_some() as u8,
        write_batch_size: options.write_batch_size.unwrap_or(2 << 20),
        statistics_enabled: 0,
        bloom_enabled: options
            .bloom_filter
            .as_ref()
            .is_some_and(|filter| filter.enabled) as u8,
        bloom_bits_per_key: options
            .bloom_filter
            .as_ref()
            .map_or(10.0, |filter| filter.bits_per_key),
        bloom_block_based_mode: options
            .bloom_filter
            .as_ref()
            .is_some_and(|filter| filter.block_based_mode) as u8,
    };
    resolved
        .validate()
        .map_err(|error| DataFusionError::Execution(error.into()))?;
    Ok(resolved)
}

pub(crate) fn compression(options: Option<&crate::proto::NativeRocksDbOptions>) -> Result<&[i32]> {
    let levels = options
        .and_then(|options| options.compression.as_ref())
        .map_or(&[1][..], |compression| compression.per_level.as_slice());
    streamfusion_state_abi::validate_rocksdb_compression(levels)
        .map_err(|error| DataFusionError::Execution(error.into()))?;
    Ok(levels)
}

pub(crate) fn requires_protocol_7(options: &crate::proto::NativeRocksDbOptions) -> bool {
    options.log_level.is_some() || options.bloom_filter.is_some() || options.compression.is_some()
}
