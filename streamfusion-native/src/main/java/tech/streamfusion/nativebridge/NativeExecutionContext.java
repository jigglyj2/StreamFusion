/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

import java.util.Objects;

/** Task-scoped native plan, runtime, and DataFusion memory-pool owner. */
public final class NativeExecutionContext implements AutoCloseable {
    private static final java.util.concurrent.atomic.AtomicLong OPENED_STREAMS =
            NativeExecutionDiagnostics.PLAN_STREAMS;

    static {
        NativeLibraryLoader.load();
    }

    private long handle;
    private final long rootPlanNodeId;
    private final byte[] identifiedPlan;
    private final boolean stateful;
    private final boolean region;
    private final boolean inputEnvelopeRequired;

    public NativeExecutionContext(byte[] serializedPlan, NativeMemoryManager memoryManager) {
        this(serializedPlan, memoryManager, null);
    }

    /** Binds Flink-owned resources to identified physical nodes before any native execution. */
    public NativeExecutionContext(byte[] serializedPlan, NativeMemoryManager memoryManager, byte[] stateBindings) {
        this(serializedPlan, memoryManager, stateBindings, null);
    }

    /** Binds keyed state and non-keyed Flink execution resources before capability negotiation. */
    public NativeExecutionContext(
            byte[] serializedPlan, NativeMemoryManager memoryManager, byte[] stateBindings, byte[] taskBindings) {
        this(serializedPlan, memoryManager, stateBindings, taskBindings, false);
    }

    public static NativeExecutionContext region(byte[] plan, NativeMemoryManager memory, byte[] state, byte[] task) {
        return new NativeExecutionContext(plan, memory, state, task, true);
    }

    private NativeExecutionContext(
            byte[] serializedPlan,
            NativeMemoryManager memoryManager,
            byte[] stateBindings,
            byte[] taskBindings,
            boolean region) {
        this.region = region;
        stateful = stateBindings != null;
        Objects.requireNonNull(serializedPlan, "serializedPlan");
        Objects.requireNonNull(memoryManager, "memoryManager");
        if (memoryManager.limit() <= 0) {
            throw new IllegalArgumentException("Native memory limit must be positive");
        }
        identifiedPlan = region ? serializedPlan.clone() : NativePlanNodeIdentity.assign(serializedPlan);
        rootPlanNodeId = region ? 0 : NativePlanNodeIdentity.rootId(identifiedPlan);
        long controlBytes = Math.addExact(
                Math.addExact((long) identifiedPlan.length, stateBindings == null ? 0 : stateBindings.length),
                taskBindings == null ? 0 : taskBindings.length);
        if (!memoryManager.tryReserve(controlBytes)) {
            throw new IllegalStateException(
                    "Flink denied " + controlBytes + " bytes for native plan/state-binding JNI copies");
        }
        try {
            if (region) {
                if (NativeRegionStream.edgeVersion() != 2)
                    throw new IllegalStateException("Unsupported native region C Data edge version");
                handle = NativeRegionStream.createContext(
                        identifiedPlan, stateBindings, taskBindings, memoryManager, memoryManager.limit());
            } else if (taskBindings != null) {
                handle = NativeTaskResources.create(
                        identifiedPlan, stateBindings, taskBindings, memoryManager, memoryManager.limit());
            } else {
                handle = stateBindings == null
                        ? createExecutionContext(identifiedPlan, memoryManager, memoryManager.limit())
                        : NativePlanState.create(identifiedPlan, stateBindings, memoryManager, memoryManager.limit());
            }
        } finally {
            memoryManager.release(controlBytes);
        }
        if (handle == 0) {
            throw new IllegalStateException("Native execution context returned a null handle");
        }
        try {
            inputEnvelopeRequired = readInputEnvelopeRequirement(handle);
        } catch (RuntimeException | Error failure) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    long handle() {
        if (handle == 0) {
            throw new IllegalStateException("Native execution context is closed");
        }
        return handle;
    }

    /** Control-plane access to independently named state in this one execution tree. */
    public NativePlanState state() {
        return new NativePlanState(this);
    }

    public boolean hasRegionOutputs() {
        return region;
    }

    public boolean hasStateBindings() {
        return stateful;
    }

    /** Record metadata is required by task-lifetime kernels, even without a keyed backend. */
    public boolean requiresInputEnvelope() {
        return inputEnvelopeRequired;
    }

    public long metricValue(String name) {
        if (region) throw new IllegalStateException("Region metrics require an explicit physical stage ID");
        return metricValue(rootPlanNodeId, name);
    }

    /** Stable identities for the same tree used by native execution and metric snapshots. */
    public byte[] identifiedPlan() {
        return identifiedPlan.clone();
    }

    /** Reads cumulative (plan ID, logical input rows, logical output rows) triples in one call. */
    public long[] metricSnapshot() {
        return readMetricSnapshot(handle());
    }

    /** Stable per-stage control capabilities, discovered once after native state binding. */
    public byte[] controlCapabilities() {
        return readControlCapabilities(handle());
    }

    /** Gauge names/types/scopes in snapshot order; discover once after state binding. */
    public byte[] gaugeSchema() {
        return readGaugeSchema(handle());
    }

    /** Samples all declared gauges together; no per-gauge JNI from metric reporters. */
    public long[] gaugeSnapshot() {
        return readGaugeSnapshot(handle());
    }

    /** One invocation of any native tree, with C Data inputs and a bounded C Stream output. */
    public void executeArrowStream(long[] inputArrays, long[] inputSchemas, long outputStream) {
        executeArrowStream(inputArrays, inputSchemas, null, outputStream);
    }

    /** Versioned, stage-addressed Flink controls through the same retained native tree. */
    public void executeArrowControlStream(long[] inputArrays, long[] inputSchemas, byte[] controls, long outputStream) {
        executeArrowStream(inputArrays, inputSchemas, Objects.requireNonNull(controls, "controls"), outputStream);
    }

    private void executeArrowStream(long[] inputArrays, long[] inputSchemas, byte[] controls, long outputStream) {
        Objects.requireNonNull(inputArrays, "inputArrays");
        Objects.requireNonNull(inputSchemas, "inputSchemas");
        if (inputArrays.length != inputSchemas.length || outputStream == 0) {
            throw new IllegalArgumentException("Invalid native plan Arrow edge addresses");
        }
        if (controls == null) executeArrowStreamInputs(handle(), inputArrays, inputSchemas, outputStream);
        else executeArrowControlStreamInputs(handle(), inputArrays, inputSchemas, controls, outputStream);
        OPENED_STREAMS.incrementAndGet();
    }

    /** Decode one network frame directly into this native plan; returns its logical input rows. */
    public long executeExchangeStream(
            int port,
            byte[] plan,
            byte[] payload,
            int offset,
            int length,
            int metadataLength,
            long[] arrays,
            long[] schemas,
            long output) {
        long rows = executeExchangeStreamInputs(
                handle(), port, plan, payload, offset, length, metadataLength, arrays, schemas, output);
        OPENED_STREAMS.incrementAndGet();
        return rows;
    }

    private static native long executeExchangeStreamInputs(
            long handle,
            int port,
            byte[] plan,
            byte[] payload,
            int offset,
            int length,
            int metadataLength,
            long[] arrays,
            long[] schemas,
            long output);

    /** Process-local diagnostic: successfully opened plan streams, independent of operator family. */
    public static long openedStreamCount() {
        return OPENED_STREAMS.get();
    }

    public static void resetStreamMetrics() {
        OPENED_STREAMS.set(0);
    }

    /** Returns one physical operator's native metric without including child operators. */
    public long metricValue(long planNodeId, String name) {
        Objects.requireNonNull(name, "name");
        if (planNodeId <= 0) {
            throw new IllegalArgumentException("Native plan node id must be positive");
        }
        return readMetricValue(handle(), planNodeId, name);
    }

    @Override
    public synchronized void close() {
        if (handle != 0) {
            closeExecutionContext(handle);
            handle = 0;
        }
    }

    private static native long createExecutionContext(
            byte[] serializedPlan, NativeMemoryManager memoryManager, long memoryLimit);

    private static native void closeExecutionContext(long handle);

    private static native long readMetricValue(long handle, long planNodeId, String name);

    private static native long[] readMetricSnapshot(long handle);

    private static native byte[] readControlCapabilities(long handle);

    private static native boolean readInputEnvelopeRequirement(long handle);

    private static native byte[] readGaugeSchema(long handle);

    private static native long[] readGaugeSnapshot(long handle);

    static native int nativeGaugeEdgeVersion();

    private static native void executeArrowStreamInputs(
            long handle, long[] inputArrays, long[] inputSchemas, long outputStream);

    private static native void executeArrowControlStreamInputs(
            long handle, long[] inputArrays, long[] inputSchemas, byte[] controls, long outputStream);

    static native int nativeControlEdgeVersion();

    static native int nativePlanProtocolVersion();

    static native int nativeStreamEdgeVersion();
}
