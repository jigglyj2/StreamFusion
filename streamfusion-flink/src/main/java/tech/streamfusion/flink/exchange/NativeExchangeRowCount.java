/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;

/** Header-only inspection; the receiving native edge still decodes Arrow IPC payload exactly once. */
final class NativeExchangeRowCount {
    private NativeExchangeRowCount() {}

    static int read(byte[] payload, int offset, int length, int bodyLength) {
        try {
            var header = ByteBuffer.wrap(payload, offset, length).slice().order(ByteOrder.LITTLE_ENDIAN);
            var message = Message.getRootAsMessage(header);
            if (message.headerType() != MessageHeader.RecordBatch || message.bodyLength() != bodyLength)
                throw new IllegalArgumentException("Expected an Arrow record-batch header with matching body length");
            var batch = (RecordBatch) message.header(new RecordBatch());
            if (batch == null || batch.length() < 0 || batch.length() > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Arrow IPC row count is outside the Java batch range");
            return (int) batch.length();
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Arrow IPC record-batch header for clock capture", failure);
        }
    }
}
