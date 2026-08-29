/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

/** One schema-free Arrow IPC frame routed to a downstream Flink subtask. */
public final class NativeExchangeFrame {
    private final int keyGroup;
    private final byte[] metadata;
    private final byte[] body;

    public NativeExchangeFrame(int keyGroup, byte[] metadata, byte[] body) {
        if (keyGroup < 0) {
            throw new IllegalArgumentException("Exchange key group must be non-negative");
        }
        this.keyGroup = keyGroup;
        this.metadata = metadata.clone();
        this.body = body.clone();
    }

    public int keyGroup() {
        return keyGroup;
    }

    public byte[] metadata() {
        return metadata.clone();
    }

    public byte[] body() {
        return body.clone();
    }
}
