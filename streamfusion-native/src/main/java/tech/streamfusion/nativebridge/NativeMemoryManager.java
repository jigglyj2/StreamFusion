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

/** Host callbacks used by native execution to reserve and release task memory. */
public interface NativeMemoryManager {
    /** Attempts to reserve bytes from the host task's memory budget. */
    boolean tryReserve(long bytes);

    /** Releases bytes previously reserved by {@link #tryReserve(long)}. */
    void release(long bytes);

    /** Bytes this consumer can currently reserve after local and task-wide limits are applied. */
    default long available() {
        return limit();
    }

    /**
     * Transfers an existing native reservation to an Arrow Java foreign-buffer import.
     *
     * <p>The default implementation releases the native reservation. Flink's managed-memory
     * implementation keeps the host reservation live and offers it as credit to the immediately
     * following Arrow import, avoiding a double charge for the same zero-copy buffers.
     */
    default void transferToArrow(long bytes) {
        release(bytes);
    }

    /** Finishes the current native-to-Arrow ownership transfer and releases unused credit. */
    default void finishArrowTransfer() {}

    /** Returns the maximum number of bytes governed by this manager. */
    long limit();

    /** Unbounded manager used only by low-level bridge tests without a Flink task. */
    static NativeMemoryManager unbounded() {
        return UnboundedNativeMemoryManager.INSTANCE;
    }

    enum UnboundedNativeMemoryManager implements NativeMemoryManager {
        INSTANCE;

        @Override
        public boolean tryReserve(long bytes) {
            return bytes >= 0;
        }

        @Override
        public void release(long bytes) {
            if (bytes < 0) {
                throw new IllegalArgumentException("Released native memory must be non-negative");
            }
        }

        @Override
        public long limit() {
            return Long.MAX_VALUE;
        }

        @Override
        public long available() {
            return Long.MAX_VALUE;
        }
    }
}
