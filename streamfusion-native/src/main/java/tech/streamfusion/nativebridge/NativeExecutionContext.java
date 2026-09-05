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
    static {
        NativeLibraryLoader.load();
    }

    private long handle;

    public NativeExecutionContext(byte[] serializedPlan, NativeMemoryManager memoryManager) {
        Objects.requireNonNull(serializedPlan, "serializedPlan");
        Objects.requireNonNull(memoryManager, "memoryManager");
        if (memoryManager.limit() <= 0) {
            throw new IllegalArgumentException("Native memory limit must be positive");
        }
        byte[] identifiedPlan = NativePlanNodeIdentity.assign(serializedPlan);
        if (!memoryManager.tryReserve(identifiedPlan.length)) {
            throw new IllegalStateException(
                    "Flink denied " + identifiedPlan.length + " bytes for the native plan JNI copy");
        }
        try {
            handle = createExecutionContext(identifiedPlan, memoryManager, memoryManager.limit());
        } finally {
            memoryManager.release(identifiedPlan.length);
        }
        if (handle == 0) {
            throw new IllegalStateException("Native execution context returned a null handle");
        }
    }

    long handle() {
        if (handle == 0) {
            throw new IllegalStateException("Native execution context is closed");
        }
        return handle;
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
}
