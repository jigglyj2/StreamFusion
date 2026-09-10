/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.memory;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;

/** Slot-scoped OPERATOR admission shared by native operators; Flink still owns every reservation. */
final class NativeOperatorMemoryPool implements AutoCloseable {
    private final MemoryManager manager;
    private final long limit;
    private long reserved;

    NativeOperatorMemoryPool(MemoryManager manager, long limit) {
        if (limit <= 0 || limit > manager.getMemorySize()) {
            throw new IllegalArgumentException("Native OPERATOR pool must fit Flink's slot managed memory");
        }
        this.manager = manager;
        this.limit = limit;
    }

    long limit() {
        return limit;
    }

    synchronized long reserved() {
        return reserved;
    }

    synchronized long available() {
        return Math.min(limit - reserved, manager.availableMemory());
    }

    synchronized boolean tryReserve(long bytes) {
        if (bytes > limit - reserved) {
            return false;
        }
        try {
            manager.reserveMemory(this, bytes);
        } catch (MemoryReservationException unavailable) {
            return false;
        }
        reserved += bytes;
        return true;
    }

    synchronized void release(long bytes) {
        if (bytes < 0 || bytes > reserved) {
            throw new IllegalStateException("Invalid shared native memory release: " + bytes);
        }
        manager.releaseMemory(this, bytes);
        reserved -= bytes;
    }

    @Override
    public synchronized void close() {
        if (reserved != 0) {
            throw new IllegalStateException("Shared native pool closed with live reservations: " + reserved);
        }
    }
}
