/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Lifecycle, timers, keyed state, and Arrow C Data boundary for native interval joins. */
public final class NativeIntervalJoinBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private NativeIntervalJoinBridge() {}

    public static long create(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager manager) {
        long handle = createHandle(plan, maxParallelism, firstKeyGroup, lastKeyGroup, manager, manager.limit());
        if (handle == 0) {
            throw new IllegalStateException("Native interval join returned a null handle");
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
            NativeMemoryManager manager) {
        long handle = createRocksHandle(
                plan,
                maxParallelism,
                firstKeyGroup,
                lastKeyGroup,
                NativeDeduplicateBridge.rocksDbLibraryPath().toString(),
                databasePath.toString(),
                manager,
                memoryLimit);
        if (handle == 0) {
            throw new IllegalStateException("Native RocksDB interval join returned a null handle");
        }
        return handle;
    }

    public static long process(
            long handle,
            int side,
            long processingTime,
            long inputArray,
            long inputSchema,
            long outputArray,
            long outputSchema) {
        long rows = processArrowBatch(handle, side, processingTime, inputArray, inputSchema, outputArray, outputSchema);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
    }

    public static long advance(
            long handle, boolean processingTime, long timestamp, long outputArray, long outputSchema) {
        return advanceTime(handle, processingTime ? 1 : 0, timestamp, outputArray, outputSchema);
    }

    public static long nextProcessingTimeTimer(long handle) {
        return nextProcessingTimer(handle);
    }

    public static long[] statistics(long handle) {
        return nativeStatistics(handle);
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
            long handle, Path checkpoint, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        importRocksCheckpointHandle(
                handle,
                NativeDeduplicateBridge.rocksDbLibraryPath().toString(),
                checkpoint.toString(),
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
            NativeMemoryManager manager,
            long memoryLimit);

    private static native long createRocksHandle(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            String pluginPath,
            String databasePath,
            NativeMemoryManager manager,
            long memoryLimit);

    private static native long processArrowBatch(
            long handle,
            int side,
            long processingTime,
            long inputArray,
            long inputSchema,
            long outputArray,
            long outputSchema);

    private static native long advanceTime(
            long handle, int processingTime, long timestamp, long outputArray, long outputSchema);

    private static native long nextProcessingTimer(long handle);

    private static native long[] nativeStatistics(long handle);

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
