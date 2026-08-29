/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

/** JNI boundary for Flink-compatible native Arrow exchange routing. */
public final class NativeExchangeBridge {
    static {
        NativeLibraryLoader.load();
    }

    private NativeExchangeBridge() {}

    public static byte[] routeArrowBatch(byte[] serializedPlan, long inputArrayAddress, long inputSchemaAddress) {
        return routeArrowBatch(serializedPlan, inputArrayAddress, inputSchemaAddress, NativeMemoryManager.unbounded());
    }

    public static native byte[] routeArrowBatch(
            byte[] serializedPlan, long inputArrayAddress, long inputSchemaAddress, NativeMemoryManager memoryManager);

    public static long decodeArrowBatch(
            byte[] serializedPlan, byte[] metadata, byte[] body, long outputArrayAddress, long outputSchemaAddress) {
        return decodeArrowBatch(
                serializedPlan,
                metadata,
                body,
                outputArrayAddress,
                outputSchemaAddress,
                NativeMemoryManager.unbounded());
    }

    public static native long decodeArrowBatch(
            byte[] serializedPlan,
            byte[] metadata,
            byte[] body,
            long outputArrayAddress,
            long outputSchemaAddress,
            NativeMemoryManager memoryManager);
}
