/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.core.memory.DataOutputView;

/** One schema-free Arrow IPC frame routed to a downstream Flink subtask. */
public final class NativeExchangeFrame {
    private final int keyGroup;
    private final byte[] payload;
    private final int metadataOffset;
    private final int metadataLength;
    private final int bodyOffset;
    private final int bodyLength;

    public NativeExchangeFrame(int keyGroup, byte[] metadata, byte[] body) {
        if (keyGroup < 0) {
            throw new IllegalArgumentException("Exchange key group must be non-negative");
        }
        this.keyGroup = keyGroup;
        this.payload = new byte[Math.addExact(metadata.length, body.length)];
        System.arraycopy(metadata, 0, payload, 0, metadata.length);
        System.arraycopy(body, 0, payload, metadata.length, body.length);
        this.metadataOffset = 0;
        this.metadataLength = metadata.length;
        this.bodyOffset = metadata.length;
        this.bodyLength = body.length;
    }

    private NativeExchangeFrame(
            int keyGroup, byte[] payload, int metadataOffset, int metadataLength, int bodyOffset, int bodyLength) {
        if (keyGroup < 0) {
            throw new IllegalArgumentException("Exchange key group must be non-negative");
        }
        this.keyGroup = keyGroup;
        this.payload = payload;
        this.metadataOffset = metadataOffset;
        this.metadataLength = metadataLength;
        this.bodyOffset = bodyOffset;
        this.bodyLength = bodyLength;
    }

    static NativeExchangeFrame wrapPayload(
            int keyGroup, byte[] payload, int metadataOffset, int metadataLength, int bodyOffset, int bodyLength) {
        if (metadataOffset < 0
                || metadataLength < 0
                || bodyOffset < 0
                || bodyLength < 0
                || metadataOffset > payload.length - metadataLength
                || bodyOffset > payload.length - bodyLength) {
            throw new IllegalArgumentException("Exchange frame payload range is invalid");
        }
        return new NativeExchangeFrame(keyGroup, payload, metadataOffset, metadataLength, bodyOffset, bodyLength);
    }

    public int keyGroup() {
        return keyGroup;
    }

    public byte[] metadata() {
        return Arrays.copyOfRange(payload, metadataOffset, metadataOffset + metadataLength);
    }

    public byte[] body() {
        return Arrays.copyOfRange(payload, bodyOffset, bodyOffset + bodyLength);
    }

    public int metadataLength() {
        return metadataLength;
    }

    int bodyLength() {
        return bodyLength;
    }

    /** Returns one contiguous IPC message/body payload for the native decoder. */
    public byte[] ipcPayload() {
        byte[] ipcPayload = new byte[Math.addExact(metadataLength, bodyLength)];
        System.arraycopy(payload, metadataOffset, ipcPayload, 0, metadataLength);
        System.arraycopy(payload, bodyOffset, ipcPayload, metadataLength, bodyLength);
        return ipcPayload;
    }

    void writePayloadTo(DataOutputView target) throws IOException {
        target.write(payload, metadataOffset, metadataLength);
        target.write(payload, bodyOffset, bodyLength);
    }
}
