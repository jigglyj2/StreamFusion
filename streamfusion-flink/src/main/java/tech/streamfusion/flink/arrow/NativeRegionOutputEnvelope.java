/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrames;

/** Versioned JNI output tag; IPC payloads stay in the original Java transport array. */
final class NativeRegionOutputEnvelope {
    final int port;
    final int rows;
    final List<NativeExchangeFrame> frames;

    private NativeRegionOutputEnvelope(int port, int rows, List<NativeExchangeFrame> frames) {
        this.port = port;
        this.rows = rows;
        this.frames = frames;
    }

    static NativeRegionOutputEnvelope read(byte[] bytes) {
        if (bytes == null) return null;
        if (bytes.length < 16) throw new IllegalStateException("Truncated native output header");
        var header = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        if (header.getInt() != 1) throw new IllegalStateException("Unsupported native output header version");
        int kind = header.getInt();
        int port = header.getInt();
        int rows = header.getInt();
        if (port < 0 || rows < 0) throw new IllegalStateException("Invalid native output port or row count");
        if (kind == 1 && bytes.length == 16) return new NativeRegionOutputEnvelope(port, rows, null);
        if (kind == 2) return new NativeRegionOutputEnvelope(port, rows, NativeExchangeFrames.decode(bytes, 16));
        throw new IllegalStateException("Invalid native output transport kind");
    }
}
