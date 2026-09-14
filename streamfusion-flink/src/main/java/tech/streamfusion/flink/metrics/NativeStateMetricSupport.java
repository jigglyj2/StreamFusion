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
            NativeRocksDbStatisticsConfiguration.fromConfig(config);
        } catch (UnsupportedOperationException failure) {
            return failure.getMessage();
        } catch (ReflectiveOperationException | LinkageError failure) {
            return "metrics: cannot resolve Flink RocksDB native metric configuration: "
                    + failure.getClass().getSimpleName();
        }
        return null;
    }
}
