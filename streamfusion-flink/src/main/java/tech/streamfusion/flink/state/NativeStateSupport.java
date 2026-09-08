/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import org.apache.flink.configuration.ReadableConfig;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;

/** Common configuration and metric preflight before any native keyed region is selected. */
public final class NativeStateSupport {
    private NativeStateSupport() {}

    public static String unsupportedReason(ReadableConfig config) {
        String reason = NativeStateConfigurationSupport.unsupportedReason(config);
        return reason != null ? reason : NativeStateMetricSupport.unsupportedReason(config);
    }
}
