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
        byte[] payload = new byte[Math.addExact(metadata.length, body.length)];
        System.arraycopy(metadata, 0, payload, 0, metadata.length);
        System.arraycopy(body, 0, payload, metadata.length, body.length);
        return decodeArrowBatch(
                serializedPlan,
                payload,
                0,
                payload.length,
                metadata.length,
                outputArrayAddress,
                outputSchemaAddress,
                NativeMemoryManager.unbounded());
    }

    public static native long decodeArrowBatch(
            byte[] serializedPlan,
            byte[] payload,
            int payloadOffset,
            int payloadLength,
            int metadataLength,
            long outputArrayAddress,
            long outputSchemaAddress,
            NativeMemoryManager memoryManager);
}
