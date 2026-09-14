/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import org.apache.flink.configuration.ReadableConfig;

/** Matches Flink's disabled manual-compaction branch without claiming a native scheduler. */
final class NativeRocksDbManualCompactionConfiguration {
    private static final String PREFIX = "state.backend.rocksdb.manual-compaction.";
    static final String INTERVAL = PREFIX + "min-interval";
    private static final Set<String> KEYS = Set.of(
            INTERVAL,
            PREFIX + "max-parallel-compactions",
            PREFIX + "max-file-size-to-compact",
            PREFIX + "min-files-to-compact",
            PREFIX + "max-files-to-compact",
            PREFIX + "max-output-file-size",
            PREFIX + "max-auto-compactions");

    private NativeRocksDbManualCompactionConfiguration() {}

    static Set<String> keys() {
        return KEYS;
    }

    static void validate(ReadableConfig config) throws ReflectiveOperationException {
        Class<?> type = Class.forName(
                "org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionConfig",
                false,
                Thread.currentThread().getContextClassLoader());
        final Object resolved;
        try {
            // Flink parses every setting even when disabled; retain its errors, units and millisecond truncation.
            resolved = type.getMethod("from", ReadableConfig.class).invoke(null, config);
        } catch (InvocationTargetException failure) {
            if (failure.getCause() instanceof RuntimeException) throw (RuntimeException) failure.getCause();
            throw failure;
        }
        if (type.getField("minInterval").getLong(resolved) > 0) {
            throw new UnsupportedOperationException(
                    "native RocksDB does not yet propagate " + INTERVAL
                            + "; Flink's small-SST selection and manual compaction scheduler are not connected to native databases");
        }
    }
}
