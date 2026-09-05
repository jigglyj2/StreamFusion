/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.util.concurrent.atomic.AtomicLong;

/** JNI lifecycle for the native local mini-batch aggregate. */
public final class NativeLocalGroupAggregateBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeLocalGroupAggregateBridge() {}

    public static long create(byte[] serializedPlan, NativeMemoryManager memoryManager) {
        return createHandle(serializedPlan, memoryManager, memoryManager.limit());
    }

    public static long process(long handle, long inputArray, long inputSchema, long outputArray, long outputSchema) {
        long rows = processArrowBatch(handle, inputArray, inputSchema, outputArray, outputSchema);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long finishBundle(long handle, long outputArray, long outputSchema) {
        long rows = finishBundleNative(handle, outputArray, outputSchema);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    public static long pendingElementCount(long handle) {
        return pendingElementCountNative(handle);
    }

    public static long pendingKeyCount(long handle) {
        return pendingKeyCountNative(handle);
    }

    public static void destroy(long handle) {
        destroyHandle(handle);
    }

    private static native long createHandle(byte[] serializedPlan, NativeMemoryManager memoryManager, long memoryLimit);

    private static native long processArrowBatch(
            long handle, long inputArray, long inputSchema, long outputArray, long outputSchema);

    private static native long finishBundleNative(long handle, long outputArray, long outputSchema);

    private static native long pendingElementCountNative(long handle);

    private static native long pendingKeyCountNative(long handle);

    private static native void destroyHandle(long handle);
}
