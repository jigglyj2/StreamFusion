/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameSerializer;
import tech.streamfusion.nativebridge.NativeMemoryManager;

final class SharedChannelStateIO {
    static void capture(StreamTaskMailboxTestHarness<RowData> task, int gate, int channel, NativeExchangeFrame frame)
            throws Exception {
        // TestInputChannel has no network capture implementation. Hand its exact serialized
        // frame to Flink's real channel-state writer, as LocalInputChannel does during alignment.
        var serializer = new StreamElementSerializer<>(NativeExchangeFrameSerializer.INSTANCE);
        var delegate = new SerializationDelegate<StreamElement>(serializer);
        delegate.setInstance(new StreamRecord<>(frame));
        var serialized = RecordWriter.serializeRecord(new DataOutputSerializer(4096), delegate);
        byte[] bytes = new byte[serialized.remaining()];
        serialized.get(bytes);
        Buffer buffer = new NetworkBuffer(MemorySegmentFactory.wrap(bytes), FreeingBufferRecycler.INSTANCE);
        buffer.setSize(bytes.length);
        task.getStreamMockEnvironment()
                .getChannelStateWriter()
                .addInputData(
                        1,
                        new InputChannelInfo(gate, channel),
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        CloseableIterator.ofElement(buffer, Buffer::recycleBuffer));
    }

    static final class RoutingMemory implements NativeMemoryManager {
        private long reserved;

        @Override
        public synchronized boolean tryReserve(long bytes) {
            if (bytes < 0 || bytes > limit() - reserved) return false;
            reserved += bytes;
            return true;
        }

        @Override
        public synchronized void release(long bytes) {
            if (bytes < 0 || bytes > reserved) throw new IllegalStateException("Invalid routing memory release");
            reserved -= bytes;
        }

        @Override
        public long limit() {
            return 64L << 20;
        }

        @Override
        public synchronized long available() {
            return limit() - reserved;
        }
    }
}
