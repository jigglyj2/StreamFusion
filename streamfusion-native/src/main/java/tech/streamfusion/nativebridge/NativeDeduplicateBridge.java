/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Lifecycle and Arrow C Data boundary for one persistent native deduplicate operator. */
public final class NativeDeduplicateBridge {
    private static final String ROCKSDB_RESOURCE = "/META-INF/native/linux-x86_64/libstreamfusion_state_rocksdb.so";
    private static Path extractedRocksDbLibrary;

    static {
        NativeLibraryLoader.load();
    }

    private NativeDeduplicateBridge() {}

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
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, Path databasePath, long memoryLimit) {
        long handle = createRocksHandle(
                plan,
                maxParallelism,
                firstKeyGroup,
                lastKeyGroup,
                extractRocksDbLibrary().toString(),
                databasePath.toString(),
                memoryLimit);
        if (handle == 0) {
            throw new IllegalStateException("Native RocksDB deduplicate returned a null handle");
        }
        return handle;
    }

    /** Returns whether the independently packaged native RocksDB component is on the classpath. */
    public static boolean isRocksDbAvailable() {
        return NativeDeduplicateBridge.class.getResource(ROCKSDB_RESOURCE) != null;
    }

    private static synchronized Path extractRocksDbLibrary() {
        if (extractedRocksDbLibrary != null) {
            return extractedRocksDbLibrary;
        }
        try (InputStream library = NativeDeduplicateBridge.class.getResourceAsStream(ROCKSDB_RESOURCE)) {
            if (library == null) {
                throw new IllegalStateException(
                        "Flink selected RocksDB state, but streamfusion-state-rocksdb is not on the classpath");
            }
            Path extracted = Files.createTempFile("streamfusion-state-rocksdb-", ".so");
            Files.copy(library, extracted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            extracted.toFile().deleteOnExit();
            extractedRocksDbLibrary = extracted.toAbsolutePath();
            return extractedRocksDbLibrary;
        } catch (IOException error) {
            throw new IllegalStateException("Could not extract the native RocksDB state component", error);
        }
    }

    public static long process(
            long handle,
            long inputArrayAddress,
            long inputSchemaAddress,
            long outputArrayAddress,
            long outputSchemaAddress) {
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
        return processOutputArrowBatch(
                handle, inputArrayAddress, inputSchemaAddress, outputArrayAddress, outputSchemaAddress);
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
                extractRocksDbLibrary().toString(),
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
