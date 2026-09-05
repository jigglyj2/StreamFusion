/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.util.concurrent.atomic.AtomicLong;

/** Persistent native BatchExecRank lifecycle and Arrow C Data boundary. */
public final class NativeBoundedRankBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeBoundedRankBridge() {}

    public static long create(byte[] plan, NativeMemoryManager memoryManager) {
        long handle = createHandle(plan, memoryManager);
        if (handle == 0) {
            throw new IllegalStateException("Native bounded rank returned a null handle");
        }
        return handle;
    }

    public static long process(long handle, long inputArray, long inputSchema, long outputArray, long outputSchema) {
        long rows = processArrowBatch(handle, inputArray, inputSchema, outputArray, outputSchema);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long[] statistics(long handle) {
        return nativeStatistics(handle);
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    /** Records a bounded-rank invocation implemented by the shared keyed Top-N kernel. */
    public static void recordExecutedBatch() {
        EXECUTED_BATCHES.incrementAndGet();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    public static void destroy(long handle) {
        destroyHandle(handle);
    }

    private static native long createHandle(byte[] plan, NativeMemoryManager memoryManager);

    private static native long processArrowBatch(
            long handle, long inputArray, long inputSchema, long outputArray, long outputSchema);

    private static native long[] nativeStatistics(long handle);

    private static native void destroyHandle(long handle);
}
