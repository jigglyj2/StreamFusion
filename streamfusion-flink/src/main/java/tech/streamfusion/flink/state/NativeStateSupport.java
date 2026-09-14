/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;
import tech.streamfusion.nativebridge.NativeStateResources;

/** Common configuration and metric preflight before any native keyed region is selected. */
public final class NativeStateSupport {
    private NativeStateSupport() {}

    public static String unsupportedReason(ReadableConfig config) {
        String reason = NativeStateConfigurationSupport.unsupportedReason(config);
        if (reason != null) return reason;
        reason = unsupportedControlAndMetrics(config);
        if (reason != null) return reason;
        if (StreamFusionStateBackendFactory.configuredBackend(config).equals("rocksdb")) {
            reason = NativeStateResources.rocksDbUnsupportedReason();
            if (reason != null) return "state backend: " + reason;
        }
        return null;
    }

    static String unsupportedControlAndMetrics(ReadableConfig config) {
        String reason = NativeStateMetricSupport.unsupportedReason(config);
        if (reason != null) return reason;
        if (config.get(CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED)) {
            return "state backend: native keyed regions do not support checkpointing during channel recovery ("
                    + CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED.key()
                    + "): Flink 2.3 local channels can recapture unread recovered buffers before their barrier";
        }
        return null;
    }
}
