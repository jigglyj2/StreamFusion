/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.util.Objects;

/** One cooperative native invocation with port-tagged, independently typed Arrow C Data outputs. */
public final class NativeRegionStream implements AutoCloseable {
    static {
        NativeLibraryLoader.load();
    }

    public static int edgeVersion() {
        return nativeEdgeVersion();
    }

    private long handle;

    private NativeRegionStream(long handle) {
        if (handle == 0) throw new IllegalStateException("Native region returned a null output handle");
        this.handle = handle;
    }

    public static NativeRegionStream open(
            NativeExecutionContext context, long[] arrays, long[] schemas, byte[] controls) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(arrays, "arrays");
        Objects.requireNonNull(schemas, "schemas");
        if (!context.hasRegionOutputs() || arrays.length != schemas.length)
            throw new IllegalArgumentException("Native region requires its own plan and matching input ports");
        var stream = new NativeRegionStream(open(context.handle(), arrays, schemas, controls));
        NativeExecutionDiagnostics.PLAN_STREAMS.incrementAndGet();
        return stream;
    }
    /** Returns -1 at EOF, otherwise the output port. The schema is exported on each port's first batch. */
    public synchronized int next(long array, long schema) {
        if (handle == 0) throw new IllegalStateException("Native region output is closed");
        if (array == 0 || schema == 0)
            throw new IllegalArgumentException("Native region output requires fresh C Data descriptors");
        return nextBatch(handle, array, schema);
    }

    @Override
    public synchronized void close() {
        if (handle != 0) {
            release(handle);
            handle = 0;
        }
    }

    private static native int nativeEdgeVersion();

    static native long createContext(byte[] plan, byte[] state, byte[] task, NativeMemoryManager memory, long limit);

    private static native long open(long context, long[] arrays, long[] schemas, byte[] controls);

    private static native int nextBatch(long handle, long array, long schema);

    private static native void release(long handle);
}
