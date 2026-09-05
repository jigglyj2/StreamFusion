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

/** JNI boundary for vectorized native calc execution. */
public final class NativeCalcBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeCalcBridge() {}

    public static long executeArrow(
            byte[] serializedPlan,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress,
            NativeMemoryManager memoryManager) {
        try (NativeExecutionContext context = new NativeExecutionContext(serializedPlan, memoryManager)) {
            return executeArrow(
                    context, inputArrayAddress, inputSchemaAddress, outputArrayAddress, outputSchemaAddress);
        }
    }

    public static void executeArrowStream(
            NativeExecutionContext context, long inputArrayAddress, long inputSchemaAddress, long outputStreamAddress) {
        executeArrowStreamBatch(context.handle(), inputArrayAddress, inputSchemaAddress, outputStreamAddress);
        EXECUTED_BATCHES.incrementAndGet();
    }

    public static long executeArrow(
            NativeExecutionContext context,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress) {
        long rows = executeArrowBatch(
                context.handle(), inputArrayAddress, inputSchemaAddress, outputArrayAddress, outputSchemaAddress);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    /** Records Calc stages executed inside another operator's fused native plan. */
    public static void recordFusedBatches(long batches) {
        if (batches < 0) {
            throw new IllegalArgumentException("Fused Calc batch count must be non-negative");
        }
        EXECUTED_BATCHES.addAndGet(batches);
    }

    private static native long executeArrowBatch(
            long executionContext,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress);

    private static native void executeArrowStreamBatch(
            long executionContext, long inputArrayAddress, long inputSchemaAddress, long outputStreamAddress);
}
