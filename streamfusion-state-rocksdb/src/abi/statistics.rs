// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::{backend, operation};
use rocksdb::{statistics::Ticker, Options};
use std::ffi::c_void;
use std::ptr;
use streamfusion_state_abi::{validate_rocksdb_tickers, ROCKSDB_TICKER_NAMES};

unsafe extern "C" {
    pub(super) fn streamfusion_rocksdb_statistics_memory_required() -> usize;
}

// Options was cloned before attaching table/cache/WBM resources. Its upstream shared_ptr
// keeps statistics alive independently of the database and the Flink memory lease.
pub(super) struct StatisticsReader(Options);

pub(super) unsafe fn owner<'a>(handle: *const c_void) -> Option<&'a Options> {
    unsafe {
        handle
            .cast::<StatisticsReader>()
            .as_ref()
            .map(|reader| &reader.0)
    }
}

pub(super) unsafe extern "C" fn open_statistics(
    handle: *mut c_void,
    output: *mut *mut c_void,
) -> i32 {
    if !output.is_null() {
        unsafe { ptr::write(output, ptr::null_mut()) };
    }
    operation(|| {
        if output.is_null() {
            return Err("RocksDB statistics output address is null".into());
        }
        let options = backend(handle)?
            .statistics_reader()
            .map_err(|e| e.to_string())?;
        unsafe {
            ptr::write(
                output,
                Box::into_raw(Box::new(StatisticsReader(options))).cast(),
            )
        };
        Ok(())
    })
}

pub(super) unsafe extern "C" fn read_statistics(
    handle: *const c_void,
    codes: *const u32,
    count: usize,
    values: *mut u64,
) -> i32 {
    operation(|| {
        let options = unsafe { owner(handle) }
            .ok_or_else(|| "RocksDB statistics reader is null".to_string())?;
        if count > ROCKSDB_TICKER_NAMES.len() {
            return Err("RocksDB statistics request exceeds eleven tickers".into());
        }
        if count == 0 {
            return Ok(());
        }
        if codes.is_null() || values.is_null() {
            return Err("RocksDB statistics ticker/value address is null".into());
        }
        let codes = unsafe { std::slice::from_raw_parts(codes, count) };
        validate_rocksdb_tickers(codes)?;
        // Validate the entire request before writing any caller-owned output.
        for (index, &code) in codes.iter().enumerate() {
            unsafe { ptr::write(values.add(index), options.get_ticker_count(ticker(code))) };
        }
        Ok(())
    })
}

pub(super) unsafe extern "C" fn close_statistics(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle.cast::<StatisticsReader>())) };
    }
}

fn ticker(code: u32) -> Ticker {
    match code {
        0 => Ticker::BlockCacheHit,
        1 => Ticker::BlockCacheMiss,
        2 => Ticker::BloomFilterUseful,
        3 => Ticker::BloomFilterFullPositive,
        4 => Ticker::BloomFilterFullTruePositive,
        5 => Ticker::BytesRead,
        6 => Ticker::IterBytesRead,
        7 => Ticker::BytesWritten,
        8 => Ticker::CompactReadBytes,
        9 => Ticker::CompactWriteBytes,
        10 => Ticker::StallMicros,
        _ => unreachable!("validated ABI ticker code"),
    }
}

#[cfg(test)]
mod tests;
