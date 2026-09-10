/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.util.Arrays;

/** One completion-edge read of stage counters, typed gauge values, and optional timer deadlines. */
public final class NativeInvocationSnapshot {
    private final long[] metrics;
    private final long[] gauges;
    private final long[] deadlines;

    NativeInvocationSnapshot(long[] encoded) {
        if (encoded == null
                || encoded.length < 4
                || encoded[0] != 1
                || encoded[1] < 0
                || encoded[2] < 0
                || encoded[3] < 0
                || encoded[1] > encoded.length
                || encoded[2] > encoded.length
                || encoded[3] > encoded.length
                || encoded[1] % 3 != 0
                || encoded[3] % 2 != 0
                || encoded[1] + encoded[2] + encoded[3] != encoded.length - 4L) {
            throw new IllegalArgumentException("Invalid native invocation snapshot");
        }
        int metricsEnd = 4 + (int) encoded[1];
        int gaugesEnd = metricsEnd + (int) encoded[2];
        metrics = Arrays.copyOfRange(encoded, 4, metricsEnd);
        gauges = Arrays.copyOfRange(encoded, metricsEnd, gaugesEnd);
        deadlines = Arrays.copyOfRange(encoded, gaugesEnd, encoded.length);
    }

    public long[] metrics() {
        return metrics;
    }

    public long[] gauges() {
        return gauges;
    }

    public long[] deadlines() {
        return deadlines;
    }
}
