/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Lifecycle, keyed state, and Arrow boundary for native N-input streaming join. */
public final class NativeMultiJoinBridge {
    private static final AtomicLong EXECUTED_BATCHES = new AtomicLong();

    static {
        NativeLibraryLoader.load();
    }

    private static final NativeKeyedStateBridge KEYED_STATE_BRIDGE = NativeKeyedStateBridge.of(
            NativeMultiJoinBridge::create,
            NativeMultiJoinBridge::createRocksDb,
            NativeMultiJoinBridge::snapshot,
            NativeMultiJoinBridge::restore,
            NativeMultiJoinBridge::checkpointRocks,
            NativeMultiJoinBridge::importRocksCheckpoint,
            NativeMultiJoinBridge::destroy);

    private NativeMultiJoinBridge() {}

    public static NativeKeyedStateBridge keyedStateBridge() {
        return KEYED_STATE_BRIDGE;
    }

    public static long create(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager manager) {
        long handle = createHandle(plan, maxParallelism, firstKeyGroup, lastKeyGroup, manager, manager.limit());
        if (handle == 0) {
            throw new IllegalStateException("Native multi-join returned a null handle");
        }
        return handle;
    }

    public static long createRocksDb(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            Path database,
            long limit,
            NativeMemoryManager manager) {
        long handle = createRocksHandle(
                plan,
                maxParallelism,
                firstKeyGroup,
                lastKeyGroup,
                NativeRocksDbLibrary.path().toString(),
                database.toString(),
                manager,
                limit);
        if (handle == 0) {
            throw new IllegalStateException("Native RocksDB multi-join returned a null handle");
        }
        return handle;
    }

    public static long process(
            long handle, int input, long inputArray, long inputSchema, long outputArray, long outputSchema) {
        long rows = processArrowBatch(handle, input, inputArray, inputSchema, outputArray, outputSchema);
        EXECUTED_BATCHES.incrementAndGet();
        return rows;
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

    public static byte[] snapshot(long handle, int group) {
        return snapshotKeyGroup(handle, group);
    }

    public static void restore(long handle, int group, byte[] bytes) {
        restoreKeyGroup(handle, group, bytes);
    }

    public static void checkpointRocks(long handle, Path directory) {
        checkpointRocksHandle(handle, directory.toString());
    }

    public static void importRocksCheckpoint(long handle, Path checkpoint, int first, int last, long limit) {
        importRocksCheckpointHandle(
                handle, NativeRocksDbLibrary.path().toString(), checkpoint.toString(), first, last, limit);
    }

    public static void destroy(long handle) {
        destroyHandle(handle);
    }

    private static native long createHandle(
            byte[] plan, int maxParallelism, int first, int last, NativeMemoryManager manager, long limit);

    private static native long createRocksHandle(
            byte[] plan,
            int maxParallelism,
            int first,
            int last,
            String plugin,
            String database,
            NativeMemoryManager manager,
            long limit);

    private static native long processArrowBatch(
            long handle, int input, long inputArray, long inputSchema, long outputArray, long outputSchema);

    private static native long[] nativeStatistics(long handle);

    private static native byte[] snapshotKeyGroup(long handle, int group);

    private static native void restoreKeyGroup(long handle, int group, byte[] bytes);

    private static native void checkpointRocksHandle(long handle, String directory);

    private static native void importRocksCheckpointHandle(
            long handle, String plugin, String checkpoint, int first, int last, long limit);

    private static native void destroyHandle(long handle);
}
