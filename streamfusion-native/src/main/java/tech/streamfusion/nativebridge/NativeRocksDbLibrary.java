/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;

/** Locates and validates the independently packaged native RocksDB state component. */
final class NativeRocksDbLibrary {
    private NativeRocksDbLibrary() {}

    static boolean isAvailable() {
        return unsupportedReason() == null;
    }

    static String unsupportedReason() {
        try {
            path();
            return null;
        } catch (IllegalStateException failure) {
            return failure.getMessage();
        }
    }

    static Path path() {
        return NativeArtifact.resolve("state_rocksdb");
    }
}
