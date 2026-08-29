/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/** Decodes the compact JNI envelope around schema-free Arrow IPC frames. */
public final class NativeExchangeFrames {
    private static final int FRAME_HEADER_BYTES = 3 * Integer.BYTES;

    private NativeExchangeFrames() {}

    public static List<NativeExchangeFrame> decode(byte[] encoded) {
        ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
        int count = readLength(input, "frame count");
        if (count > input.remaining() / FRAME_HEADER_BYTES) {
            throw new IllegalArgumentException("Native exchange frame count exceeds its JNI envelope");
        }
        List<NativeExchangeFrame> frames = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            int keyGroup = readLength(input, "key group");
            int metadataLength = readLength(input, "metadata length");
            int bodyLength = readLength(input, "body length");
            if (metadataLength > input.remaining() || bodyLength > input.remaining() - metadataLength) {
                throw new IllegalArgumentException("Native exchange frame exceeds its JNI envelope");
            }
            byte[] metadata = new byte[metadataLength];
            byte[] body = new byte[bodyLength];
            input.get(metadata);
            input.get(body);
            frames.add(new NativeExchangeFrame(keyGroup, metadata, body));
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("Native exchange JNI envelope has trailing bytes");
        }
        return List.copyOf(frames);
    }

    private static int readLength(ByteBuffer input, String label) {
        if (input.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("Native exchange JNI envelope is missing " + label);
        }
        int value = input.getInt();
        if (value < 0) {
            throw new IllegalArgumentException("Native exchange " + label + " exceeds Java's array limit");
        }
        return value;
    }
}
