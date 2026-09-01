/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.memory;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Shares one Flink operator managed-memory allowance between Arrow and native execution. */
public final class FlinkManagedMemory implements AllocationListener, NativeMemoryManager, AutoCloseable {
    private final MemoryManager memoryManager;
    private final Object reservationOwner = new Object();
    private final long limit;
    private final RootAllocator rootAllocator;
    private final BufferAllocator allocator;
    private final ThreadLocal<Deque<AllocationCharge>> preAllocationCharges = ThreadLocal.withInitial(ArrayDeque::new);

    private long reserved;
    private long peakReserved;
    private long pendingArrowTransfer;
    private boolean closed;

    public static FlinkManagedMemory create(
            Environment environment, StreamConfig operatorConfig, OperatorMetricGroup metricGroup, String name) {
        double fraction = operatorConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.OPERATOR,
                environment.getJobConfiguration(),
                environment.getTaskManagerInfo().getConfiguration(),
                environment.getUserCodeClassLoader().asClassLoader());
        MemoryManager memoryManager = environment.getMemoryManager();
        long limit = memoryManager.computeMemorySize(fraction);
        if (limit <= 0) {
            throw new IllegalStateException(
                    "Flink assigned no OPERATOR managed memory to StreamFusion; declare a positive managed-memory weight");
        }
        FlinkManagedMemory managedMemory = new FlinkManagedMemory(memoryManager, limit, name);
        MetricGroup streamFusionMetrics = metricGroup.addGroup("StreamFusion");
        streamFusionMetrics.gauge("managedMemoryUsed", managedMemory::reserved);
        streamFusionMetrics.gauge("managedMemoryPeak", managedMemory::peakReserved);
        streamFusionMetrics.gauge("managedMemoryLimit", managedMemory::limit);
        return managedMemory;
    }

    FlinkManagedMemory(MemoryManager memoryManager, long limit, String name) {
        if (limit <= 0) {
            throw new IllegalArgumentException("StreamFusion managed-memory limit must be positive");
        }
        this.memoryManager = memoryManager;
        this.limit = limit;
        this.rootAllocator = new RootAllocator(this, limit);
        this.allocator = rootAllocator.newChildAllocator(name, 0, limit);
    }

    public BufferAllocator allocator() {
        return allocator;
    }

    @Override
    public synchronized boolean tryReserve(long bytes) {
        checkNonNegative(bytes);
        if (closed) {
            throw new IllegalStateException("StreamFusion managed memory is closed");
        }
        if (bytes == 0) {
            return true;
        }
        if (bytes > limit - reserved) {
            return false;
        }
        try {
            memoryManager.reserveMemory(reservationOwner, bytes);
        } catch (MemoryReservationException unavailable) {
            return false;
        }
        reserved += bytes;
        peakReserved = Math.max(peakReserved, reserved);
        return true;
    }

    @Override
    public synchronized void release(long bytes) {
        checkNonNegative(bytes);
        if (bytes == 0) {
            return;
        }
        if (bytes > reserved) {
            throw new IllegalStateException(
                    "StreamFusion attempted to release " + bytes + " bytes with only " + reserved + " reserved");
        }
        memoryManager.releaseMemory(reservationOwner, bytes);
        reserved -= bytes;
    }

    @Override
    public synchronized void transferToArrow(long bytes) {
        checkNonNegative(bytes);
        if (closed) {
            throw new IllegalStateException("StreamFusion managed memory is closed");
        }
        if (pendingArrowTransfer != 0) {
            throw new IllegalStateException(
                    "A native-to-Arrow memory transfer is already pending: " + pendingArrowTransfer + " bytes");
        }
        if (bytes > reserved) {
            throw new IllegalStateException(
                    "StreamFusion attempted to transfer " + bytes + " bytes with only " + reserved + " reserved");
        }
        pendingArrowTransfer = bytes;
    }

    @Override
    public synchronized void finishArrowTransfer() {
        long unused = pendingArrowTransfer;
        pendingArrowTransfer = 0;
        release(unused);
    }

    @Override
    public long limit() {
        return limit;
    }

    public synchronized long reserved() {
        return reserved;
    }

    public synchronized long peakReserved() {
        return peakReserved;
    }

    @Override
    public synchronized void onPreAllocation(long size) {
        checkNonNegative(size);
        long transferred = Math.min(size, pendingArrowTransfer);
        long additional = size - transferred;
        if (!tryReserve(additional)) {
            throw new OutOfMemoryException("Flink denied "
                    + additional
                    + " new Arrow bytes for a "
                    + size
                    + " byte allocation; "
                    + reserved()
                    + " of "
                    + limit
                    + " bytes are reserved");
        }
        pendingArrowTransfer -= transferred;
        preAllocationCharges.get().push(new AllocationCharge(size, transferred, additional));
    }

    @Override
    public void onAllocation(long size) {
        AllocationCharge charge = preAllocationCharges.get().pop();
        if (charge.size != size) {
            throw new IllegalStateException(
                    "Arrow allocation callback size changed from " + charge.size + " to " + size);
        }
    }

    @Override
    public boolean onFailedAllocation(long size, org.apache.arrow.memory.AllocationOutcome outcome) {
        AllocationCharge charge = preAllocationCharges.get().pop();
        if (charge.size != size) {
            throw new IllegalStateException(
                    "Arrow failed-allocation callback size changed from " + charge.size + " to " + size);
        }
        synchronized (this) {
            pendingArrowTransfer += charge.transferred;
        }
        release(charge.additional);
        return false;
    }

    @Override
    public void onRelease(long size) {
        release(size);
    }

    @Override
    public void close() {
        RuntimeException allocatorFailure = null;
        try {
            allocator.close();
        } catch (RuntimeException failure) {
            allocatorFailure = failure;
        }
        try {
            rootAllocator.close();
        } catch (RuntimeException failure) {
            if (allocatorFailure == null) {
                allocatorFailure = failure;
            } else {
                allocatorFailure.addSuppressed(failure);
            }
        } finally {
            synchronized (this) {
                memoryManager.releaseAllMemory(reservationOwner);
                reserved = 0;
                pendingArrowTransfer = 0;
                closed = true;
            }
        }
        if (allocatorFailure != null) {
            throw allocatorFailure;
        }
    }

    private static void checkNonNegative(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Managed-memory byte count must be non-negative");
        }
    }

    private static final class AllocationCharge {
        private final long size;
        private final long transferred;
        private final long additional;

        private AllocationCharge(long size, long transferred, long additional) {
            this.size = size;
            this.transferred = transferred;
            this.additional = additional;
        }
    }
}
