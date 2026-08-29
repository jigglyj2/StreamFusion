/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

/** One schema-free Arrow IPC frame routed to a downstream Flink subtask. */
public final class NativeExchangeFrame {
    private final int destination;
    private final byte[] metadata;
    private final byte[] body;

    public NativeExchangeFrame(int destination, byte[] metadata, byte[] body) {
        if (destination < 0) {
            throw new IllegalArgumentException("Exchange destination must be non-negative");
        }
        this.destination = destination;
        this.metadata = metadata.clone();
        this.body = body.clone();
    }

    public int destination() {
        return destination;
    }

    public byte[] metadata() {
        return metadata.clone();
    }

    public byte[] body() {
        return body.clone();
    }
}
