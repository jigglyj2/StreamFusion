// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

fn main() {
    let root = "vendor/rocksdb/librocksdb-sys/rocksdb";
    println!("cargo:rerun-if-changed=src/statistics_memory.cc");
    for header in [
        "monitoring/statistics_impl.h",
        "monitoring/histogram.h",
        "include/rocksdb/statistics.h",
        "util/core_local.h",
        "port",
    ] {
        println!("cargo:rerun-if-changed={root}/{header}");
    }
    let target = std::env::var("TARGET").unwrap();
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .file("src/statistics_memory.cc")
        .include(root)
        .include(format!("{root}/include"));
    if target.contains("windows") {
        build.define("OS_WIN", None).define("NOMINMAX", None);
    } else {
        build.define("ROCKSDB_PLATFORM_POSIX", None);
        if target.contains("darwin") || target.contains("apple-ios") {
            build.define("OS_MACOSX", None);
        }
    }
    build.compile("streamfusion_statistics_memory");
}
