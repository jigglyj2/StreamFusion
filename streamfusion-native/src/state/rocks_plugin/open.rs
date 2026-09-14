// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use streamfusion_state_abi::{
    InitializeStateBackend, StateBackendOpenOptions, STATE_BACKEND_ABI_VERSION,
};

impl RocksPluginKeyedState {
    #[cfg(test)]
    pub(crate) fn open(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
    ) -> Result<Self> {
        Self::open_scoped(
            library_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            [0, 0],
            None,
            false,
            0.5,
            0.1,
            None,
            false,
            &[],
            None,
            None,
        )
    }

    pub(crate) fn open_checkpoint(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
    ) -> Result<Self> {
        Self::open_scoped(
            library_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            [0, 0],
            None,
            true,
            0.5,
            0.1,
            None,
            false,
            &[],
            None,
            None,
        )
    }

    pub(crate) fn open_for_owner(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
        owner: &HostMemoryReservation,
    ) -> Result<Self> {
        Self::open_scoped(
            library_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            owner.rocks_scope()?,
            None,
            false,
            0.5,
            0.1,
            None,
            false,
            &[],
            None,
            None,
        )
    }

    pub(crate) fn open_configured(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
    ) -> Result<Self> {
        Self::open_configured_mode(
            resources,
            first_key_group,
            last_key_group,
            owner,
            false,
            None,
        )
    }

    pub(crate) fn open_checkpoint_configured(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
        statistics: Option<&RocksPluginStatistics>,
    ) -> Result<Self> {
        Self::open_configured_mode(
            resources,
            first_key_group,
            last_key_group,
            owner,
            true,
            statistics,
        )
    }

    fn open_configured_mode(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
        checkpoint: bool,
        statistics: Option<&RocksPluginStatistics>,
    ) -> Result<Self> {
        Self::open_scoped(
            Path::new(&resources.plugin_path),
            Path::new(&resources.database_path),
            first_key_group,
            last_key_group,
            resources.memory_limit as usize,
            owner
                .map(HostMemoryReservation::rocks_scope)
                .transpose()?
                .unwrap_or([0, 0]),
            resources.log_directory.as_deref(),
            checkpoint,
            resources.write_buffer_ratio.unwrap_or(0.5),
            resources.high_priority_pool_ratio.unwrap_or(0.1),
            resources.database_options.as_ref(),
            resources.partitioned_index_filters.unwrap_or(false),
            &resources.statistics_tickers,
            statistics,
            owner,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_scoped(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
        scope: [u64; 2],
        log_directory: Option<&str>,
        checkpoint: bool,
        write_buffer_ratio: f64,
        high_priority_pool_ratio: f64,
        database_options: Option<&crate::proto::NativeRocksDbOptions>,
        partitioned_index_filters: bool,
        tickers: &[u32],
        statistics: Option<&RocksPluginStatistics>,
        statistics_admission: Option<&HostMemoryReservation>,
    ) -> Result<Self> {
        streamfusion_state_abi::validate_rocksdb_tickers(tickers).map_err(DataFusionError::Plan)?;
        let log_directory = database_options
            .and_then(|options| options.log_directory.as_deref())
            .or(log_directory);
        let compression = super::database_options::compression(database_options)?;
        let mut database_options = super::database_options::resolve(database_options)?;
        database_options.partitioned_index_filters = partitioned_index_filters as u8;
        database_options.statistics_enabled = (!tickers.is_empty()) as u8;
        let library = unsafe { Library::new(library_path) }
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let initialize = unsafe {
            library
                .get::<InitializeStateBackend>(b"streamfusion_state_backend_init\0")
                .map_err(|error| DataFusionError::External(Box::new(error)))?
        };
        let mut api = ptr::null();
        let status = unsafe { initialize(STATE_BACKEND_ABI_VERSION, &mut api) };
        if status != STATE_BACKEND_OK || api.is_null() {
            return Err(DataFusionError::Execution(format!(
                "RocksDB state plugin rejected ABI version {STATE_BACKEND_ABI_VERSION}"
            )));
        }
        let api = unsafe { &*api };
        if api.abi_version != STATE_BACKEND_ABI_VERSION {
            return Err(DataFusionError::Execution(format!(
                "RocksDB state plugin returned ABI version {}",
                api.abi_version
            )));
        }
        if statistics.is_some_and(|reader| !std::ptr::eq(reader.api, api)) {
            return Err(DataFusionError::Execution(
                "RocksDB statistics owner belongs to a different component".into(),
            ));
        }
        let statistics_memory =
            super::statistics::admit(api, !tickers.is_empty(), statistics, statistics_admission)?;
        let database_path = database_path.to_str().ok_or_else(|| {
            DataFusionError::Execution("RocksDB state path is not UTF-8".to_string())
        })?;
        let log_directory = log_directory.unwrap_or_default();
        let options = StateBackendOpenOptions {
            struct_size: std::mem::size_of::<StateBackendOpenOptions>(),
            path: database_path.as_ptr(),
            path_len: database_path.len(),
            first_key_group,
            last_key_group,
            memory_limit,
            memory_scope_high: scope[0],
            memory_scope_low: scope[1],
            log_directory: log_directory.as_ptr(),
            log_directory_len: log_directory.len(),
            write_buffer_ratio,
            high_priority_pool_ratio,
            database_options,
            compression_per_level: compression.as_ptr(),
            compression_per_level_len: compression.len(),
            statistics_owner: statistics.map_or(ptr::null(), |reader| reader.handle),
        };
        let mut handle = ptr::null_mut();
        let open = if checkpoint {
            api.open_checkpoint
        } else {
            api.open
        };
        let status = unsafe { open(&options, &mut handle) };
        check(api, status)?;
        if handle.is_null() {
            return Err(DataFusionError::Execution(
                "RocksDB state plugin returned a null handle".to_string(),
            ));
        }
        Ok(Self {
            library: Arc::new(library),
            api,
            handle,
            statistics_memory,
        })
    }
}
