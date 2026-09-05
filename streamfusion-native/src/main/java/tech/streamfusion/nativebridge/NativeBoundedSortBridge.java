/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Lifecycle, state, and Arrow C Data boundary for native bounded full sort. */
public final class NativeBoundedSortBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private static final NativeKeyedStateBridge KEYED_STATE_BRIDGE = NativeKeyedStateBridge.of(
            (plan, ignoredMaxParallelism, firstKeyGroup, lastKeyGroup, memoryManager) ->
                    create(plan, firstKeyGroup, lastKeyGroup, memoryManager),
            (plan, ignoredMaxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager) ->
                    createRocksDb(plan, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager),
            NativeBoundedSortBridge::snapshot,
            NativeBoundedSortBridge::restore,
            NativeBoundedSortBridge::checkpointRocks,
            NativeBoundedSortBridge::importRocksCheckpoint,
            NativeBoundedSortBridge::destroy);

    private NativeBoundedSortBridge() {}

    public static NativeKeyedStateBridge keyedStateBridge() {
        return KEYED_STATE_BRIDGE;
    }

    public static long create(byte[] plan, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        long handle = createHandle(plan, firstKeyGroup, lastKeyGroup, memoryManager, memoryManager.limit());
        if (handle == 0) {
            throw new IllegalStateException("Native bounded sort returned a null handle");
        }
        return handle;
    }

    public static long createRocksDb(
            byte[] plan,
            int firstKeyGroup,
            int lastKeyGroup,
            Path databasePath,
            long memoryLimit,
            NativeMemoryManager memoryManager) {
        long handle = createRocksHandle(
                plan,
                firstKeyGroup,
                lastKeyGroup,
                NativeRocksDbLibrary.path().toString(),
                databasePath.toString(),
                memoryManager,
                memoryLimit);
        if (handle == 0) {
            throw new IllegalStateException("Native RocksDB bounded sort returned a null handle");
        }
        return handle;
    }

    public static void process(long handle, long inputArray, long inputSchema) {
        processArrowBatch(handle, inputArray, inputSchema);
        EXECUTED_BATCHES.incrementAndGet();
    }

    public static long finish(long handle, long outputArray, long outputSchema) {
        return finishArrow(handle, outputArray, outputSchema);
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
            long targetHandle, Path checkpointPath, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        importRocksCheckpointHandle(
                targetHandle,
                NativeRocksDbLibrary.path().toString(),
                checkpointPath.toString(),
                firstKeyGroup,
                lastKeyGroup,
                memoryLimit);
    }

    public static void destroy(long handle) {
        destroyHandle(handle);
    }

    private static native long createHandle(
            byte[] plan, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager, long memoryLimit);

    private static native long createRocksHandle(
            byte[] plan,
            int firstKeyGroup,
            int lastKeyGroup,
            String pluginPath,
            String databasePath,
            NativeMemoryManager memoryManager,
            long memoryLimit);

    private static native void processArrowBatch(long handle, long inputArray, long inputSchema);

    private static native long finishArrow(long handle, long outputArray, long outputSchema);

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
