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

    public static native byte[] routeArrowBatch(byte[] serializedPlan, long inputArrayAddress, long inputSchemaAddress);

    public static native long decodeArrowBatch(
            byte[] serializedPlan, byte[] metadata, byte[] body, long outputArrayAddress, long outputSchemaAddress);
}
