/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

/** Versioned task-open snapshot edge; source configuration and data never enter the plan protobuf. */
final class NativeLookupResources {
    private NativeLookupResources() {}

    static native int edgeVersion();

    static native long create(
            byte[] plan,
            byte[] state,
            byte[] task,
            long[] nodeIds,
            long[] streams,
            NativeMemoryManager manager,
            long limit,
            boolean region);
}
