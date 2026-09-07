/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateLatencyTrackOptions;

/** Exact configuration admission for metric surfaces not yet provided by shared native state. */
public final class NativeStateMetricSupport {
    private NativeStateMetricSupport() {}

    public static String unsupportedReason(ReadableConfig config) {
        if (config.get(StateLatencyTrackOptions.LATENCY_TRACK_ENABLED)) {
            return "metrics: keyed-state latency histograms are not yet published by shared native state";
        }
        try {
            // RocksDB remains an optional Flink integration. Use its own option resolver,
            // including defaults/aliases, rather than maintaining a second option-name list.
            Class<?> options = Class.forName(
                    "org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions",
                    false,
                    Thread.currentThread().getContextClassLoader());
            Object resolved =
                    options.getMethod("fromConfig", ReadableConfig.class).invoke(null, config);
            if ((boolean) options.getMethod("isEnabled").invoke(resolved)) {
                return "metrics: enabled RocksDB native metrics are not yet published by shared native state";
            }
        } catch (ClassNotFoundException absentOptionalBackend) {
            // No RocksDB implementation can be selected without that optional integration.
        } catch (ReflectiveOperationException | LinkageError failure) {
            return "metrics: cannot resolve Flink RocksDB native metric configuration: "
                    + failure.getClass().getSimpleName();
        }
        return null;
    }
}
