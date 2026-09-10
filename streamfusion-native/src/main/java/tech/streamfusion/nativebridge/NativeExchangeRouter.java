/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.util.Objects;

/** Task-scoped exchange plan and reservation broker, reused across Arrow input batches. */
public final class NativeExchangeRouter implements AutoCloseable {
    static {
        NativeLibraryLoader.load();
    }

    private long handle;

    public NativeExchangeRouter(byte[] plan, NativeMemoryManager memory) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(memory, "memory");
        if (edgeVersion() != 1) throw new IllegalStateException("Unsupported native exchange router edge version");
        handle = create(plan, memory);
        if (handle == 0) throw new IllegalStateException("Native exchange router returned a null handle");
    }

    public synchronized byte[] route(long array, long schema) {
        if (handle == 0) throw new IllegalStateException("Native exchange router is closed");
        return route(handle, array, schema);
    }

    @Override
    public synchronized void close() {
        if (handle == 0) return;
        long owned = handle;
        handle = 0;
        closeRouter(owned);
    }

    public static native int edgeVersion();

    private static native long create(byte[] plan, NativeMemoryManager memory);

    private static native byte[] route(long handle, long array, long schema);

    private static native void closeRouter(long handle);
}
