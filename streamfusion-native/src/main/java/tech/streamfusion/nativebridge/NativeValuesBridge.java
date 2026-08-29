/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

import java.util.concurrent.atomic.AtomicLong;

/** JNI boundary for source-free native VALUES execution. */
public final class NativeValuesBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeValuesBridge() {}

    public static long executeArrow(byte[] serializedPlan, long outputArrayAddress, long outputSchemaAddress) {
        try (NativeExecutionContext context =
                new NativeExecutionContext(serializedPlan, NativeMemoryManager.unbounded())) {
            return executeArrow(context, outputArrayAddress, outputSchemaAddress);
        }
    }

    public static long executeArrow(NativeExecutionContext context, long outputArrayAddress, long outputSchemaAddress) {
        long rows = executeArrowBatch(context.handle(), outputArrayAddress, outputSchemaAddress);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    private static native long executeArrowBatch(
            long executionContext, long outputArrayAddress, long outputSchemaAddress);
}
