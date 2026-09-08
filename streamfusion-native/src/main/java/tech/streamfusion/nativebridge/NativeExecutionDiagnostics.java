/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.util.concurrent.atomic.AtomicLong;

/** Process-local diagnostic counters; observing them must never initialize native execution. */
public final class NativeExecutionDiagnostics {
    static final AtomicLong PLAN_STREAMS = new AtomicLong();
    static final AtomicLong CALC_BATCHES = new AtomicLong();

    private NativeExecutionDiagnostics() {}

    public static long planStreams() {
        return PLAN_STREAMS.get();
    }

    public static long calcBatches() {
        return CALC_BATCHES.get();
    }

    public static void reset() {
        PLAN_STREAMS.set(0);
        CALC_BATCHES.set(0);
    }
}
