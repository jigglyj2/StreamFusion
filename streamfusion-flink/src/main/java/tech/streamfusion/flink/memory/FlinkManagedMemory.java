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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Tracks an operator's Arrow/native ownership within the slot's shared OPERATOR allowance. */
public final class FlinkManagedMemory implements AllocationListener, NativeMemoryManager, AutoCloseable {
    private final OpaqueMemoryResource<NativeOperatorMemoryPool> poolLease;
    private final NativeOperatorMemoryPool pool;
    private final long limit;
    private final long assignedOperatorShare;
    private final RootAllocator rootAllocator;
    private final BufferAllocator allocator;
    private final ThreadLocal<Deque<AllocationCharge>> preAllocationCharges = ThreadLocal.withInitial(ArrayDeque::new);

    private java.util.UUID rocksDbMemoryScope = java.util.UUID.randomUUID();

    /** Bind before native state creation to the shared STATE_BACKEND reservation identity. */
    public void shareRocksDbMemoryScope(java.util.UUID scope) {
        this.rocksDbMemoryScope = java.util.Objects.requireNonNull(scope);
    }

    @Override
    public long rocksDbMemoryScopeHigh() {
        return rocksDbMemoryScope.getMostSignificantBits();
    }

    @Override
    public long rocksDbMemoryScopeLow() {
        return rocksDbMemoryScope.getLeastSignificantBits();
    }

    private long reserved;
    private long peakReserved;
    private long pendingArrowTransfer;
    private boolean closed;
    private boolean leaseClosed;

    public static FlinkManagedMemory create(
            Environment environment, StreamConfig operatorConfig, OperatorMetricGroup metricGroup, String name) {
        double fraction = operatorConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.OPERATOR,
                environment.getJobConfiguration(),
                environment.getTaskManagerInfo().getConfiguration(),
                environment.getUserCodeClassLoader().asClassLoader());
        MemoryManager memoryManager = environment.getMemoryManager();
        long assignedShare = memoryManager.computeMemorySize(fraction);
        if (assignedShare <= 0) {
            throw new IllegalStateException(
                    "Flink assigned no OPERATOR managed memory to StreamFusion; declare a positive managed-memory weight");
        }
        // Use Flink's own use-case membership, backend flag, weights and rounding, but without
        // subdividing OPERATOR into private native ceilings. Never mutate the task's config:
        // original Flink buffer geometry continues to use its original operator share.
        StreamConfig sharedConfig = new StreamConfig(new Configuration(operatorConfig.getConfiguration()));
        sharedConfig.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
        long limit = memoryManager.computeMemorySize(sharedConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.OPERATOR,
                environment.getJobConfiguration(),
                environment.getTaskManagerInfo().getConfiguration(),
                environment.getUserCodeClassLoader().asClassLoader()));
        FlinkManagedMemory managedMemory =
                new FlinkManagedMemory(memoryManager, limit, assignedShare, "job-" + environment.getJobID(), name);
        MetricGroup streamFusionMetrics = metricGroup.addGroup("StreamFusion");
        streamFusionMetrics.gauge("managedMemoryUsed", managedMemory::reserved);
        streamFusionMetrics.gauge("managedMemoryPeak", managedMemory::peakReserved);
        streamFusionMetrics.gauge("managedMemoryLimit", managedMemory::limit);
        streamFusionMetrics.gauge("managedMemoryPoolUsed", managedMemory.pool::reserved);
        return managedMemory;
    }

    FlinkManagedMemory(MemoryManager memoryManager, long limit, String name) {
        this(memoryManager, limit, limit, "test", name);
    }

    private FlinkManagedMemory(MemoryManager memoryManager, long limit, long assignedShare, String scope, String name) {
        this.limit = limit;
        this.assignedOperatorShare = assignedShare;
        try {
            poolLease = memoryManager.getExternalSharedMemoryResource(
                    "streamfusion-native-operator-memory-v1/" + scope,
                    ignored -> new NativeOperatorMemoryPool(memoryManager, limit),
                    0);
            pool = poolLease.getResourceHandle();
            if (pool.limit() != limit) {
                poolLease.close();
                throw new IllegalStateException("Native operators disagree on Flink's slot OPERATOR allowance");
            }
        } catch (Exception failure) {
            throw new IllegalStateException("Cannot acquire Flink's shared native OPERATOR pool", failure);
        }
        RootAllocator root = null;
        try {
            root = new RootAllocator(this, limit);
            this.allocator = root.newChildAllocator(name, 0, limit);
            this.rootAllocator = root;
        } catch (RuntimeException | Error failure) {
            try {
                if (root != null) root.close();
                poolLease.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
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
        if (!pool.tryReserve(bytes)) {
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
        pool.release(bytes);
        reserved -= bytes;
        closeLeaseIfReleased();
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

    /** Original private share, used only by legacy embedded RocksDB lease sizing, not admission. */
    public long assignedOperatorShare() {
        return assignedOperatorShare;
    }

    public synchronized long reserved() {
        return reserved;
    }

    public synchronized long peakReserved() {
        return peakReserved;
    }

    @Override
    public synchronized long available() {
        return closed ? 0 : pool.available();
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
        synchronized (this) {
            if (closed) {
                return;
            }
            // Prevent new admission, but allow outstanding native/Arrow owners to release
            // their bytes after the operator or stream that produced them has closed.
            closed = true;
            finishArrowTransfer();
        }
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
        }
        // Do not releaseAllMemory here: a retained foreign buffer may still own a native
        // reservation. A leaked owner must remain visible to Flink, not be reported as freed.
        synchronized (this) {
            closeLeaseIfReleased();
        }
        if (allocatorFailure != null) {
            throw allocatorFailure;
        }
    }

    private void closeLeaseIfReleased() {
        if (closed && reserved == 0 && !leaseClosed) {
            leaseClosed = true;
            try {
                poolLease.close();
            } catch (Exception failure) {
                throw new IllegalStateException("Cannot release shared native OPERATOR pool", failure);
            }
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
