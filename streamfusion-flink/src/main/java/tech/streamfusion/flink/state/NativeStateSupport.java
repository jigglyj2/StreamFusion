/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;

/** Common configuration and metric preflight before any native keyed region is selected. */
public final class NativeStateSupport {
    private NativeStateSupport() {}

    public static String unsupportedReason(ReadableConfig config) {
        String reason = NativeStateConfigurationSupport.unsupportedReason(config);
        if (reason != null) return reason;
        reason = NativeStateMetricSupport.unsupportedReason(config);
        if (reason != null) return reason;
        if (config.get(CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED)) {
            return "state backend: native keyed regions have not verified checkpointing during channel recovery";
        }
        if (StreamFusionStateBackendFactory.configuredBackend(config).equals("rocksdb")) {
            return "state backend: native RocksDB default cache/write-buffer ratios and database options "
                    + "are not yet equivalent to Flink; retain Flink until default configuration parity is verified";
        }
        return null;
    }
}
