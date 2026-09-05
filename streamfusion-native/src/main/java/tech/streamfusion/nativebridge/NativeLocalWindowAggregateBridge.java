/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.util.concurrent.atomic.AtomicLong;

/** JNI lifecycle for the state-free local half of two-phase window aggregation. */
public final class NativeLocalWindowAggregateBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeLocalWindowAggregateBridge() {}

    public static long create(byte[] serializedPlan, NativeMemoryManager memoryManager) {
        return createHandle(serializedPlan, memoryManager, memoryManager.limit());
    }

    public static long process(long handle, long inputArray, long inputSchema, long outputArray, long outputSchema) {
        long rows = processArrowBatch(handle, inputArray, inputSchema, outputArray, outputSchema);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    public static void destroy(long handle) {
        destroyHandle(handle);
    }

    private static native long createHandle(byte[] serializedPlan, NativeMemoryManager memoryManager, long memoryLimit);

    private static native long processArrowBatch(
            long handle, long inputArray, long inputSchema, long outputArray, long outputSchema);

    private static native void destroyHandle(long handle);
}
