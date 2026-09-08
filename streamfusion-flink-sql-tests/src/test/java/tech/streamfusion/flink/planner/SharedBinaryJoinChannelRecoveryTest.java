/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedBinaryJoinMetricFixture.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameSerializer;
import tech.streamfusion.nativebridge.NativeMemoryManager;

class SharedBinaryJoinChannelRecoveryTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void flinkReplaysInflightArrowFramesAgainstRestoredJoinState(boolean rocks, boolean unaligned) throws Exception {
        var fixture = new SharedBinaryJoinMetricFixture(false);
        try (var oracle = fixture.join(rocks);
                var calc = fixture.calc();
                var allocator = new RootAllocator(64L << 20)) {
            TaskStateSnapshot checkpoint;
            var expected = new DataOutputSerializer(128);
            var memory = new RoutingMemory();
            try (var task = SharedBinaryJoinChannelHarness.create(rocks, unaligned, null)) {
                var left = row(0, RowKind.INSERT);
                oracle.accept(0, binary(left));
                send(task, allocator, memory, 0, left);
                var options = unaligned
                        ? CheckpointOptions.unaligned(
                                CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault())
                        : CheckpointOptions.alignedNoTimeout(
                                CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
                var barrier = new CheckpointBarrier(1, 1, options);
                task.processEvent(barrier, 0);
                // Input 1 has not delivered its barrier: unaligned checkpoints must retain this
                // Arrow IPC frame in channel state, not include its mutation in the keyed snapshot.
                var right = row(1, RowKind.INSERT);
                oracle.accept(1, binary(right));
                send(task, allocator, memory, 1, right, unaligned);
                task.processEvent(barrier, 1);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (task.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                    task.processAll();
                    Thread.sleep(5);
                }
                assertThat(task.getTaskStateManager().getReportedCheckpointId()).isEqualTo(1);
                checkpoint = task.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                var channelState = checkpoint
                        .getSubtaskStateByOperatorID(SharedBinaryJoinChannelHarness.REGION)
                        .getInputChannelState();
                assertThat(channelState.isEmpty()).isEqualTo(!unaligned);
                for (var event : oracle.drain()) calc.accept(event);
                for (var event : calc.drain()) StageEventBytes.encode(OUTPUT, event, expected);
                assertThat(bytes(task)).containsExactly(expected.getCopyOfBuffer());
                if (!unaligned) expected.clear();
                task.endInput();
                task.waitForTaskCompletion();
            }
            try (var restored = SharedBinaryJoinChannelHarness.create(rocks, unaligned, checkpoint)) {
                restored.processAll();
                var retract = row(0, RowKind.DELETE);
                oracle.accept(0, binary(retract));
                send(restored, allocator, memory, 0, retract);
                for (var event : oracle.drain()) calc.accept(event);
                for (var event : calc.drain()) StageEventBytes.encode(OUTPUT, event, expected);
                assertThat(bytes(restored)).containsExactly(expected.getCopyOfBuffer());
                restored.endInput();
                restored.waitForTaskCompletion();
            }
            assertThat(memory.reserved).isZero();
        }
    }

    private static GenericRowData row(int port, RowKind kind) {
        var row = GenericRowData.of(7L, StringData.fromString("payload-" + port + "-é"));
        row.setRowKind(kind);
        return row;
    }

    private static StreamRecord<RowData> binary(RowData row) {
        return new StreamRecord<>(new RowDataSerializer(INPUT).toBinaryRow(row).copy());
    }

    private static void send(
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            RoutingMemory memory,
            int port,
            GenericRowData row)
            throws Exception {
        send(task, allocator, memory, port, row, false);
    }

    private static void send(
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            RoutingMemory memory,
            int port,
            GenericRowData row,
            boolean capture)
            throws Exception {
        try (var batch = ArrowRowDataBatch.transpose(List.of(row), INPUT, allocator)
                        .withRowKinds(new RowKind[] {row.getRowKind()});
                var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT, null)) {
            for (var frame : ArrowExchangeCDataBridge.route(
                    SharedBinaryJoinChannelHarness.exchange(), envelope.batch(), allocator, memory)) {
                if (capture) capture(task, port, frame);
                task.processElement(new StreamRecord<>(frame), port);
            }
        }
    }

    private static void capture(StreamTaskMailboxTestHarness<RowData> task, int port, NativeExchangeFrame frame)
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
                        new InputChannelInfo(port, 0),
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        CloseableIterator.ofElement(buffer, Buffer::recycleBuffer));
    }

    private static byte[] bytes(StreamTaskMailboxTestHarness<RowData> task) throws Exception {
        var bytes = new DataOutputSerializer(128);
        for (var event : task.getOutput())
            if (event instanceof StreamRecord) StageEventBytes.encode(OUTPUT, (StreamRecord<?>) event, bytes);
        return bytes.getCopyOfBuffer();
    }

    private static final class RoutingMemory implements NativeMemoryManager {
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
