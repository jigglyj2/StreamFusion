// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Same-thread allocation observations for controlled unit-test scopes. Not a production
//! allocator, cross-thread profiler, or a substitute for end-to-end allocation profiles.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct AllocationStats {
    pub(crate) live: isize,
    pub(crate) peak: usize,
}
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static STATS: Cell<AllocationStats> = const { Cell::new(AllocationStats { live: 0, peak: 0 }) };
}

struct ObservedSystem;
#[global_allocator]
static ALLOCATOR: ObservedSystem = ObservedSystem;

fn observe(delta: isize) {
    // TLS can be unavailable during thread destruction. These Cells never allocate.
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            STATS.with(|stats| {
                let mut value = stats.get();
                value.live += delta;
                value.peak = value.peak.max(value.live.max(0) as usize);
                stats.set(value);
            });
        }
    });
}

unsafe impl GlobalAlloc for ObservedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            observe(layout.size() as isize);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            observe(layout.size() as isize);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        observe(-(layout.size() as isize));
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        if !replacement.is_null() {
            observe(size as isize - layout.size() as isize);
        }
        replacement
    }
}

pub(crate) fn measure<T>(operation: impl FnOnce() -> T) -> (T, AllocationStats) {
    struct Scope;
    impl Drop for Scope {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(false));
        }
    }
    ACTIVE.with(|active| assert!(!active.get(), "allocation scopes cannot nest"));
    STATS.with(|stats| stats.set(AllocationStats::default()));
    ACTIVE.with(|active| active.set(true));
    let scope = Scope;
    let result = operation();
    drop(scope);
    (result, STATS.with(Cell::get))
}

/// Check ownership transitions inside one continuous observation, without resetting live bytes.
pub(crate) fn current() -> AllocationStats {
    ACTIVE.with(|active| assert!(active.get(), "no active allocation observation"));
    STATS.with(Cell::get)
}

#[test]
fn observations_track_live_peak_and_reset_between_scopes() {
    let (bytes, stats) = measure(|| Vec::<u8>::with_capacity(128));
    assert_eq!(stats.live, 128);
    assert_eq!(stats.peak, 128);
    drop(bytes); // Outside the measured scope; does not affect the next observation.
    let (_, stats) = measure(|| {
        let bytes = std::hint::black_box(Vec::<u8>::with_capacity(512));
        drop(bytes);
    });
    assert_eq!(stats.live, 0);
    assert_eq!(stats.peak, 512);
    let (bytes, stats) = measure(|| {
        let mut bytes = Vec::<u8>::with_capacity(16);
        bytes.reserve_exact(1024);
        bytes
    });
    assert_eq!(stats.live as usize, bytes.capacity());
    assert_eq!(stats.peak, bytes.capacity());
}
