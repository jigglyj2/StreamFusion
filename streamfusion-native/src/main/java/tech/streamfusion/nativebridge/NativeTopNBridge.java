/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Persistent Arrow execution and raw keyed-state boundary for native non-window Top-N. */
public final class NativeTopNBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeTopNBridge() {}

    public static long create(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        long handle =
                createHandle(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager, memoryManager.limit());
        if (handle == 0) {
            throw new IllegalStateException("Native Top-N returned a null handle");
        }
        return handle;
    }

    public static long createRocksDb(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            Path databasePath,
            long memoryLimit,
            NativeMemoryManager memoryManager) {
        long handle = createRocksHandle(
                plan,
                maxParallelism,
                firstKeyGroup,
                lastKeyGroup,
                NativeDeduplicateBridge.rocksDbLibraryPath().toString(),
                databasePath.toString(),
                memoryManager,
                memoryLimit);
        if (handle == 0) {
            throw new IllegalStateException("Native RocksDB Top-N returned a null handle");
        }
        return handle;
    }

    public static long process(
            long handle, long nowMillis, long inputArray, long inputSchema, long outputArray, long outputSchema) {
        EXECUTED_BATCHES.incrementAndGet();
        return processBatch(handle, nowMillis, inputArray, inputSchema, outputArray, outputSchema);
    }

    public static long[] statistics(long handle) {
        return nativeStatistics(handle);
    }

    public static boolean appendLimitSaturated(long handle) {
        return isAppendLimitSaturated(handle);
    }

    public static long executedBatchCount() {
        return EXECUTED_BATCHES.get();
    }

    public static void resetMetrics() {
        EXECUTED_BATCHES.set(0);
    }

    public static byte[] snapshot(long handle, int keyGroup) {
        return snapshotKeyGroup(handle, keyGroup);
    }

    public static void restore(long handle, int keyGroup, byte[] bytes) {
        restoreKeyGroup(handle, keyGroup, bytes);
    }

    public static void checkpointRocks(long handle, Path directory) {
        checkpointRocksHandle(handle, directory.toString());
    }

    public static void importRocksCheckpoint(
            long targetHandle, Path checkpointPath, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        importRocksCheckpointHandle(
                targetHandle,
                NativeDeduplicateBridge.rocksDbLibraryPath().toString(),
                checkpointPath.toString(),
                firstKeyGroup,
                lastKeyGroup,
                memoryLimit);
    }

    public static void destroy(long handle) {
        destroyHandle(handle);
    }

    private static native long createHandle(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            NativeMemoryManager memoryManager,
            long memoryLimit);

    private static native long createRocksHandle(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            String pluginPath,
            String databasePath,
            NativeMemoryManager memoryManager,
            long memoryLimit);

    private static native long processBatch(
            long handle, long nowMillis, long inputArray, long inputSchema, long outputArray, long outputSchema);

    private static native long[] nativeStatistics(long handle);

    private static native boolean isAppendLimitSaturated(long handle);

    private static native byte[] snapshotKeyGroup(long handle, int keyGroup);

    private static native void restoreKeyGroup(long handle, int keyGroup, byte[] bytes);

    private static native void checkpointRocksHandle(long handle, String directory);

    private static native void importRocksCheckpointHandle(
            long targetHandle,
            String pluginPath,
            String checkpointPath,
            int firstKeyGroup,
            int lastKeyGroup,
            long memoryLimit);

    private static native void destroyHandle(long handle);
}
