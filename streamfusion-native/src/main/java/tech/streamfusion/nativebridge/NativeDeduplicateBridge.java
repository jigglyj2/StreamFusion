/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Lifecycle and Arrow C Data boundary for one persistent native deduplicate operator. */
public final class NativeDeduplicateBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private static final NativeKeyedStateBridge KEYED_STATE_BRIDGE = NativeKeyedStateBridge.of(
            NativeDeduplicateBridge::create,
            NativeDeduplicateBridge::createRocksDb,
            NativeDeduplicateBridge::snapshot,
            NativeDeduplicateBridge::restore,
            NativeDeduplicateBridge::checkpointRocks,
            NativeDeduplicateBridge::importRocksCheckpoint,
            NativeDeduplicateBridge::destroy);

    private NativeDeduplicateBridge() {}

    public static NativeKeyedStateBridge keyedStateBridge() {
        return KEYED_STATE_BRIDGE;
    }

    public static long create(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        long handle =
                createHandle(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager, memoryManager.limit());
        if (handle == 0) {
            throw new IllegalStateException("Native deduplicate returned a null handle");
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
                NativeRocksDbLibrary.path().toString(),
                databasePath.toString(),
                memoryManager,
                memoryLimit);
        if (handle == 0) {
            throw new IllegalStateException("Native RocksDB deduplicate returned a null handle");
        }
        return handle;
    }

    /** Returns whether the independently packaged native RocksDB component is on the classpath. */
    public static boolean isRocksDbAvailable() {
        return NativeRocksDbLibrary.isAvailable();
    }

    public static long process(
            long handle,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress) {
        EXECUTED_BATCHES.incrementAndGet();
        return processArrowBatch(
                handle, inputArrayAddress, inputSchemaAddress, outputArrayAddress, outputSchemaAddress);
    }

    /** Returns selected visible Arrow columns followed by RowKind and input-ordinal metadata. */
    public static long processOutput(
            long handle,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress) {
        EXECUTED_BATCHES.incrementAndGet();
        return processOutputArrowBatch(
                handle, inputArrayAddress, inputSchemaAddress, outputArrayAddress, outputSchemaAddress);
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

    private static native long processArrowBatch(
            long handle,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress);

    private static native long processOutputArrowBatch(
            long handle,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress);

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
