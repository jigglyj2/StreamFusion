// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

/// A read-only upstream statistics owner. It keeps the component loaded, but retains no DB,
/// managed cache or WBM. Drop cannot race sampling because sampling borrows this owner.
pub(crate) struct RocksPluginStatistics {
    _library: Arc<Library>,
    pub(super) api: &'static StateBackendApiV1,
    pub(super) handle: *mut c_void,
    pub(super) memory: Arc<HostMemoryReservation>,
}

// The ABI permits concurrent read-only samples of upstream atomic statistics. Ownership and
// library lifetime stay inside this wrapper; no database operations use this handle.
unsafe impl Send for RocksPluginStatistics {}
unsafe impl Sync for RocksPluginStatistics {}

impl RocksPluginKeyedState {
    pub(crate) fn statistics_reader(&self) -> Result<RocksPluginStatistics> {
        let memory = self.statistics_memory.as_ref().ok_or_else(|| {
            DataFusionError::Execution(
                "RocksDB statistics reader requires its admitted memory owner".into(),
            )
        })?;
        let mut handle = ptr::null_mut();
        check(self.api, unsafe {
            (self.api.open_statistics)(self.handle, &mut handle)
        })?;
        if handle.is_null() {
            return Err(DataFusionError::Execution(
                "RocksDB returned a null statistics reader".into(),
            ));
        }
        Ok(RocksPluginStatistics {
            _library: Arc::clone(&self.library),
            api: self.api,
            handle,
            memory: Arc::clone(memory),
        })
    }
}

impl RocksPluginStatistics {
    pub(crate) fn sample(&self, codes: &[u32], values: &mut [i64]) -> Result<()> {
        streamfusion_state_abi::validate_rocksdb_tickers(codes)
            .map_err(DataFusionError::Execution)?;
        if codes.len() != values.len() {
            return Err(DataFusionError::Execution(
                "RocksDB statistics output length mismatch".into(),
            ));
        }
        let mut raw = [0u64; 11];
        check(self.api, unsafe {
            (self.api.read_statistics)(self.handle, codes.as_ptr(), codes.len(), raw.as_mut_ptr())
        })?;
        for (value, raw) in values.iter_mut().zip(raw) {
            *value = raw as i64;
        }
        Ok(())
    }
}

impl Drop for RocksPluginStatistics {
    fn drop(&mut self) {
        unsafe { (self.api.close_statistics)(self.handle) };
    }
}

/// Admission precedes the upstream statistics factory. Checkpoint readers share the existing
/// owner; the final DB/reader releases the reservation after closing its C statistics handle.
pub(super) fn admit(
    api: &'static StateBackendApiV1,
    enabled: bool,
    shared: Option<&RocksPluginStatistics>,
    owner: Option<&HostMemoryReservation>,
) -> Result<Option<Arc<HostMemoryReservation>>> {
    if !enabled {
        if shared.is_some() {
            return Err(DataFusionError::Plan(
                "Disabled RocksDB statistics cannot share an owner".into(),
            ));
        }
        return Ok(None);
    }
    if let Some(shared) = shared {
        return Ok(Some(Arc::clone(&shared.memory)));
    }
    let owner = owner.ok_or_else(|| {
        DataFusionError::Execution("RocksDB statistics require Flink memory admission".into())
    })?;
    let bytes = unsafe { (api.statistics_memory_required)() };
    if bytes == 0 {
        return Err(DataFusionError::Execution(
            "RocksDB statistics memory geometry is unsupported".into(),
        ));
    }
    let mut memory = owner.sibling("RocksDB statistics");
    memory.resize(bytes)?;
    Ok(Some(Arc::new(memory)))
}

#[cfg(test)]
mod tests;
