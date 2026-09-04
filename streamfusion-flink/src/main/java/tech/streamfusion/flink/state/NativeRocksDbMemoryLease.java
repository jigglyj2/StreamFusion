/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.StateBackend;

/** Owns a native RocksDB operator's share of Flink's STATE_BACKEND managed-memory budget. */
final class NativeRocksDbMemoryLease implements AutoCloseable {
    private static final String RESOURCE_ID = "streamfusion-rocksdb-shared-v1";

    private final OpaqueMemoryResource<SharedBudget> resource;
    private boolean closed;

    static NativeRocksDbMemoryLease reserve(StateBackend.KeyedStateBackendParameters<?> parameters) throws Exception {
        if (parameters.getManagedMemoryFraction() <= 0) {
            return null;
        }
        OpaqueMemoryResource<SharedBudget> resource = parameters
                .getEnv()
                .getMemoryManager()
                .getSharedMemoryResourceForManagedMemory(
                        RESOURCE_ID, SharedBudget::new, parameters.getManagedMemoryFraction());
        return new NativeRocksDbMemoryLease(resource);
    }

    private NativeRocksDbMemoryLease(OpaqueMemoryResource<SharedBudget> resource) {
        this.resource = resource;
    }

    long size() {
        return resource.getSize();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            try {
                resource.close();
                closed = true;
            } catch (Exception failure) {
                throw new IllegalStateException("Could not release native RocksDB managed memory", failure);
            }
        }
    }

    /** Marker handle; RocksDB's process-wide cache and write-buffer manager own the actual bytes. */
    private static final class SharedBudget implements AutoCloseable {
        private final long size;

        private SharedBudget(long size) {
            this.size = size;
        }

        @Override
        public void close() {
            // The MemoryManager releases the reserved bytes after this shared handle closes.
        }

        @Override
        public String toString() {
            return "StreamFusion native RocksDB shared budget (" + size + " bytes)";
        }
    }
}
