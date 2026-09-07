/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.Objects;

/** Node-addressed Flink control operations; data still uses the single plan Arrow C Stream edge. */
public final class NativePlanState {
    private final NativeExecutionContext context;

    NativePlanState(NativeExecutionContext context) {
        this.context = context;
    }

    public byte[] snapshot(long planNodeId, int keyGroup) {
        validate(planNodeId, keyGroup);
        return snapshot(context.handle(), planNodeId, keyGroup);
    }

    public void restore(long planNodeId, int keyGroup, byte[] canonicalState) {
        validate(planNodeId, keyGroup);
        restore(context.handle(), planNodeId, keyGroup, Objects.requireNonNull(canonicalState));
    }

    public void checkpoint(long planNodeId, Path directory) {
        validate(planNodeId, 0);
        checkpoint(
                context.handle(),
                planNodeId,
                Objects.requireNonNull(directory).toAbsolutePath().toString());
    }

    private static void validate(long planNodeId, int keyGroup) {
        if (planNodeId <= 0 || keyGroup < 0) {
            throw new IllegalArgumentException(
                    "Native state requires a positive plan-node ID and non-negative key group");
        }
    }

    /** Imports only the assigned key groups; the temporary RocksDB reader uses the task's native budget. */
    public void importCheckpoint(
            long planNodeId, Path directory, int firstKeyGroup, int lastKeyGroup, long readerMemoryLimit) {
        validate(planNodeId, firstKeyGroup);
        if (lastKeyGroup < firstKeyGroup || readerMemoryLimit <= 0) {
            throw new IllegalArgumentException("Invalid checkpoint import range or memory lease");
        }
        importCheckpoint(
                context.handle(),
                planNodeId,
                NativeRocksDbLibrary.path().toString(),
                Objects.requireNonNull(directory).toAbsolutePath().toString(),
                firstKeyGroup,
                lastKeyGroup,
                readerMemoryLimit);
    }

    static native long create(byte[] plan, byte[] bindings, NativeMemoryManager manager, long limit);

    private static native byte[] snapshot(long handle, long planNodeId, int keyGroup);

    private static native void restore(long handle, long planNodeId, int keyGroup, byte[] canonicalState);

    private static native void checkpoint(long handle, long planNodeId, String directory);

    private static native void importCheckpoint(
            long handle,
            long planNodeId,
            String plugin,
            String directory,
            int firstKeyGroup,
            int lastKeyGroup,
            long readerMemoryLimit);
}
