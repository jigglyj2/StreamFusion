/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedJoinChainFixture.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;

class SharedJoinChainChannelRecoveryTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void flinkReplaysInflightFramesThroughTwoRestoredJoins(boolean rocks, boolean unaligned) throws Exception {
        var fixture = new SharedJoinChainFixture(true);
        try (var oracle = fixture.oracle(rocks);
                var allocator = new RootAllocator(64L << 20)) {
            TaskStateSnapshot checkpoint;
            var expected = new DataOutputSerializer(128);
            var memory = new SharedChannelStateIO.RoutingMemory();
            try (var task = create(fixture, rocks, unaligned, null)) {
                var left = row(fixture, 0, RowKind.INSERT);
                oracle.accept(0, binary(fixture, left));
                send(fixture, task, allocator, memory, 0, left);
                var middle = row(fixture, 1, RowKind.INSERT);
                oracle.accept(1, binary(fixture, middle));
                send(fixture, task, allocator, memory, 1, middle);
                var options = unaligned
                        ? CheckpointOptions.unaligned(
                                CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault())
                        : CheckpointOptions.alignedNoTimeout(
                                CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
                var barrier = new CheckpointBarrier(1, 1, options);
                task.processEvent(barrier, 0);
                // Input 2 has not delivered its barrier: unaligned checkpoints must retain this
                // Arrow IPC frame in channel state, not include its mutation in the keyed snapshot.
                var right = row(fixture, 2, RowKind.INSERT);
                oracle.accept(2, binary(fixture, right));
                send(fixture, task, allocator, memory, 2, right, unaligned);
                task.processEvent(barrier, 1);
                task.processEvent(barrier, 2);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (task.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                    task.processAll();
                    Thread.sleep(5);
                }
                assertThat(task.getTaskStateManager().getReportedCheckpointId()).isEqualTo(1);
                checkpoint = task.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                var channelState = checkpoint
                        .getSubtaskStateByOperatorID(SharedKeyedChannelHarness.REGION)
                        .getInputChannelState();
                assertThat(channelState.isEmpty()).isEqualTo(!unaligned);
                for (var event : oracle.drain()) StageEventBytes.encode(fixture.output, event, expected);
                assertThat(bytes(fixture, task)).containsExactly(expected.getCopyOfBuffer());
                if (!unaligned) expected.clear();
                task.endInput();
                task.waitForTaskCompletion();
            }
            try (var restored = create(fixture, rocks, unaligned, checkpoint)) {
                restored.processAll();
                var retract = row(fixture, 0, RowKind.DELETE);
                oracle.accept(0, binary(fixture, retract));
                send(fixture, restored, allocator, memory, 0, retract);
                for (var event : oracle.drain()) StageEventBytes.encode(fixture.output, event, expected);
                assertThat(bytes(fixture, restored)).containsExactly(expected.getCopyOfBuffer());
                restored.endInput();
                restored.waitForTaskCompletion();
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    private static byte[] exchange() {
        return tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 1, 1, true);
    }

    private static StreamTaskMailboxTestHarness<RowData> create(
            SharedJoinChainFixture fixture, boolean rocks, boolean unaligned, TaskStateSnapshot restore)
            throws Exception {
        var factory = new tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory(
                fixture.inputs,
                fixture.output,
                fixture.plan(),
                fixture.stateIds(),
                java.util.Collections.nCopies(3, exchange()));
        return SharedKeyedChannelHarness.create(
                factory, fixture.output, new int[] {1, 1, 1}, rocks, unaligned, restore);
    }

    private static GenericRowData row(SharedJoinChainFixture fixture, int port, RowKind kind) {
        var row = fixture.row(7L, port);
        row.setRowKind(kind);
        return row;
    }

    private static StreamRecord<RowData> binary(SharedJoinChainFixture fixture, RowData row) {
        return new StreamRecord<>(new RowDataSerializer(INPUT).toBinaryRow(row).copy());
    }

    private static void send(
            SharedJoinChainFixture fixture,
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            int port,
            GenericRowData row)
            throws Exception {
        send(fixture, task, allocator, memory, port, row, false);
    }

    private static void send(
            SharedJoinChainFixture fixture,
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            int port,
            GenericRowData row,
            boolean capture)
            throws Exception {
        try (var batch = ArrowRowDataBatch.transpose(List.of(row), INPUT, allocator)
                        .withRowKinds(new RowKind[] {row.getRowKind()});
                var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT, null)) {
            for (var frame : ArrowExchangeCDataBridge.route(exchange(), envelope.batch(), allocator, memory)) {
                if (capture) SharedChannelStateIO.capture(task, port, 0, frame);
                task.processElement(new StreamRecord<>(frame), port);
            }
        }
    }

    private static byte[] bytes(SharedJoinChainFixture fixture, StreamTaskMailboxTestHarness<RowData> task)
            throws Exception {
        var bytes = new DataOutputSerializer(128);
        for (var event : task.getOutput())
            if (event instanceof StreamRecord) StageEventBytes.encode(fixture.output, (StreamRecord<?>) event, bytes);
        return bytes.getCopyOfBuffer();
    }
}
