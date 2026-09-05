/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.io.Serializable;
import java.nio.file.Path;

/** Serializable function table for the common native keyed-state lifecycle. */
public final class NativeKeyedStateBridge implements Serializable {
    private final MemoryFactory memoryFactory;
    private final RocksDbFactory rocksDbFactory;
    private final Snapshotter snapshotter;
    private final Restorer restorer;
    private final RocksDbCheckpointer checkpointer;
    private final RocksDbImporter importer;
    private final Destroyer destroyer;

    private NativeKeyedStateBridge(
            MemoryFactory memoryFactory,
            RocksDbFactory rocksDbFactory,
            Snapshotter snapshotter,
            Restorer restorer,
            RocksDbCheckpointer checkpointer,
            RocksDbImporter importer,
            Destroyer destroyer) {
        this.memoryFactory = memoryFactory;
        this.rocksDbFactory = rocksDbFactory;
        this.snapshotter = snapshotter;
        this.restorer = restorer;
        this.checkpointer = checkpointer;
        this.importer = importer;
        this.destroyer = destroyer;
    }

    public static NativeKeyedStateBridge of(
            MemoryFactory memoryFactory,
            RocksDbFactory rocksDbFactory,
            Snapshotter snapshotter,
            Restorer restorer,
            RocksDbCheckpointer checkpointer,
            RocksDbImporter importer,
            Destroyer destroyer) {
        return new NativeKeyedStateBridge(
                memoryFactory, rocksDbFactory, snapshotter, restorer, checkpointer, importer, destroyer);
    }

    public long createMemory(
            byte[] plan, int maxParallelism, int firstKeyGroup, int lastKeyGroup, NativeMemoryManager memoryManager) {
        return memoryFactory.create(plan, maxParallelism, firstKeyGroup, lastKeyGroup, memoryManager);
    }

    public long createRocksDb(
            byte[] plan,
            int maxParallelism,
            int firstKeyGroup,
            int lastKeyGroup,
            Path databasePath,
            long memoryLimit,
            NativeMemoryManager memoryManager) {
        return rocksDbFactory.create(
                plan, maxParallelism, firstKeyGroup, lastKeyGroup, databasePath, memoryLimit, memoryManager);
    }

    public byte[] snapshot(long handle, int keyGroup) {
        return snapshotter.snapshot(handle, keyGroup);
    }

    public void restore(long handle, int keyGroup, byte[] state) {
        restorer.restore(handle, keyGroup, state);
    }

    public void checkpointRocks(long handle, Path checkpointDirectory) {
        checkpointer.checkpoint(handle, checkpointDirectory);
    }

    public void importRocksCheckpoint(
            long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit) {
        importer.importCheckpoint(handle, checkpointDirectory, firstKeyGroup, lastKeyGroup, memoryLimit);
    }

    public void destroy(long handle) {
        destroyer.destroy(handle);
    }

    @FunctionalInterface
    public interface MemoryFactory extends Serializable {
        long create(
                byte[] plan,
                int maxParallelism,
                int firstKeyGroup,
                int lastKeyGroup,
                NativeMemoryManager memoryManager);
    }

    @FunctionalInterface
    public interface RocksDbFactory extends Serializable {
        long create(
                byte[] plan,
                int maxParallelism,
                int firstKeyGroup,
                int lastKeyGroup,
                Path databasePath,
                long memoryLimit,
                NativeMemoryManager memoryManager);
    }

    @FunctionalInterface
    public interface Snapshotter extends Serializable {
        byte[] snapshot(long handle, int keyGroup);
    }

    @FunctionalInterface
    public interface Restorer extends Serializable {
        void restore(long handle, int keyGroup, byte[] state);
    }

    @FunctionalInterface
    public interface RocksDbCheckpointer extends Serializable {
        void checkpoint(long handle, Path checkpointDirectory);
    }

    @FunctionalInterface
    public interface RocksDbImporter extends Serializable {
        void importCheckpoint(
                long handle, Path checkpointDirectory, int firstKeyGroup, int lastKeyGroup, long memoryLimit);
    }

    @FunctionalInterface
    public interface Destroyer extends Serializable {
        void destroy(long handle);
    }
}
