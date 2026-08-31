/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** Stable Flink network serializer for one schema-free Arrow IPC frame. */
public final class NativeExchangeFrameSerializer extends TypeSerializerSingleton<NativeExchangeFrame> {
    private static final long serialVersionUID = 1L;
    public static final NativeExchangeFrameSerializer INSTANCE = new NativeExchangeFrameSerializer();

    private NativeExchangeFrameSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public NativeExchangeFrame createInstance() {
        return new NativeExchangeFrame(0, new byte[0], new byte[0]);
    }

    @Override
    public NativeExchangeFrame copy(NativeExchangeFrame from) {
        return from;
    }

    @Override
    public NativeExchangeFrame copy(NativeExchangeFrame from, NativeExchangeFrame reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(NativeExchangeFrame frame, DataOutputView target) throws IOException {
        target.writeInt(frame.keyGroup());
        target.writeInt(frame.metadataLength());
        target.writeInt(frame.bodyLength());
        frame.writePayloadTo(target);
    }

    @Override
    public NativeExchangeFrame deserialize(DataInputView source) throws IOException {
        int keyGroup = nonNegative(source.readInt(), "key group");
        int metadataLength = nonNegative(source.readInt(), "metadata length");
        int bodyLength = nonNegative(source.readInt(), "body length");
        long payloadLength = (long) metadataLength + bodyLength;
        if (payloadLength > Integer.MAX_VALUE) {
            throw new IOException("Native exchange frame exceeds Java's array limit");
        }
        byte[] payload = new byte[(int) payloadLength];
        source.readFully(payload);
        return NativeExchangeFrame.wrapPayload(keyGroup, payload, 0, metadataLength, metadataLength, bodyLength);
    }

    @Override
    public NativeExchangeFrame deserialize(NativeExchangeFrame reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int keyGroup = nonNegative(source.readInt(), "key group");
        int metadataLength = nonNegative(source.readInt(), "metadata length");
        int bodyLength = nonNegative(source.readInt(), "body length");
        target.writeInt(keyGroup);
        target.writeInt(metadataLength);
        target.writeInt(bodyLength);
        long payloadLength = (long) metadataLength + bodyLength;
        if (payloadLength > Integer.MAX_VALUE) {
            throw new IOException("Native exchange frame exceeds Java's array limit");
        }
        target.write(source, (int) payloadLength);
    }

    @Override
    public TypeSerializerSnapshot<NativeExchangeFrame> snapshotConfiguration() {
        return new NativeExchangeFrameSerializerSnapshot();
    }

    private static int nonNegative(int value, String label) throws IOException {
        if (value < 0) {
            throw new IOException("Native exchange " + label + " exceeds Java's array limit");
        }
        return value;
    }

    /** Serializer snapshot for savepoint and channel-state compatibility checks. */
    public static final class NativeExchangeFrameSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<NativeExchangeFrame> {
        public NativeExchangeFrameSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
