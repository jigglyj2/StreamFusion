/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink;

import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Bounded memory manager for low-level JNI tests that do not construct a Flink task. */
public final class TestingNativeMemoryManager implements NativeMemoryManager {
    private static final long TEST_LIMIT = 1L << 30;

    private long reserved;
    private final long limit;

    public static NativeMemoryManager create() {
        return create(TEST_LIMIT);
    }

    public static NativeMemoryManager create(long limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Test memory limit must be positive");
        }
        return new TestingNativeMemoryManager(limit);
    }

    private TestingNativeMemoryManager(long limit) {
        this.limit = limit;
    }

    @Override
    public synchronized boolean tryReserve(long bytes) {
        if (bytes < 0 || bytes > limit - reserved) {
            return false;
        }
        reserved += bytes;
        return true;
    }

    @Override
    public synchronized void release(long bytes) {
        if (bytes < 0 || bytes > reserved) {
            throw new IllegalStateException("Invalid test native-memory release: " + bytes);
        }
        reserved -= bytes;
    }

    @Override
    public synchronized long available() {
        return limit - reserved;
    }

    @Override
    public long limit() {
        return limit;
    }
}
