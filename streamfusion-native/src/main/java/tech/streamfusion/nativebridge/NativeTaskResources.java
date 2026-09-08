/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

/** Constructor-time Flink resource binding; data uses the existing Arrow C Stream edge. */
final class NativeTaskResources {
    private NativeTaskResources() {}

    static native long create(
            byte[] plan, byte[] stateBindings, byte[] taskBindings, NativeMemoryManager manager, long limit);
}
