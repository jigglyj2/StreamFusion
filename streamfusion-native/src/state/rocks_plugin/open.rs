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
        )
    }

    pub(crate) fn open_configured(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
    ) -> Result<Self> {
        Self::open_configured_mode(resources, first_key_group, last_key_group, owner, false)
    }

    pub(crate) fn open_checkpoint_configured(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
    ) -> Result<Self> {
        Self::open_configured_mode(resources, first_key_group, last_key_group, owner, true)
    }

    fn open_configured_mode(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
        checkpoint: bool,
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
    ) -> Result<Self> {
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
        })
    }
}
