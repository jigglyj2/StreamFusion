/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedAppendTopNFixture.id;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Actual Flink barriers, persisted Arrow IPC channel state and native Top-N state restored together. */
class SharedAppendTopNChannelRecoveryTest {
    @ParameterizedTest(name = "rocks={0}, unaligned={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void capturedWinnerUpdatesReplayExactlyOnce(boolean rocks, boolean unaligned) throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var fixture = new SharedAppendTopNFixture(false, true, true);
            try (var first = fixture.oracle(0, rocks);
                    var top = fixture.oracle(1, rocks);
                    var tail = fixture.oracle(2, rocks);
                    var allocator = new RootAllocator(64L << 20)) {
                var oracles = List.of(first, top, tail);
                var memory = new SharedChannelStateIO.RoutingMemory();
                var expected = new DataOutputSerializer(128);
                TaskStateSnapshot checkpoint;
                try (var task = create(fixture, rocks, unaligned, null)) {
                    send(fixture, task, allocator, memory, oracles, expected, seed, 0, 0, false);
                    assertThat(bytes(fixture, task)).containsExactly(expected.getCopyOfBuffer());
                    expected.clear();
                    task.getOutput().clear();
                    var location = CheckpointStorageLocationReference.getDefault();
                    var options = unaligned
                            ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                            : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                    var barrier = new CheckpointBarrier(1, 1, options);
                    task.processEvent(barrier, 0, 0);
                    send(fixture, task, allocator, memory, oracles, expected, seed, 1, 1, unaligned);
                    task.processEvent(barrier, 0, 1);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (task.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                        task.processAll();
                        Thread.sleep(5);
                    }
                    assertThat(task.getTaskStateManager().getReportedCheckpointId())
                            .isEqualTo(1);
                    checkpoint = task.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                    assertThat(checkpoint
                                    .getSubtaskStateByOperatorID(SharedKeyedChannelHarness.REGION)
                                    .getInputChannelState()
                                    .isEmpty())
                            .isEqualTo(!unaligned);
                    assertThat(bytes(fixture, task)).containsExactly(expected.getCopyOfBuffer());
                    if (!unaligned) expected.clear();
                    task.endInput();
                    task.waitForTaskCompletion();
                }
                try (var restored = create(fixture, rocks, unaligned, checkpoint)) {
                    restored.processAll();
                    send(fixture, restored, allocator, memory, oracles, expected, seed, 2, 0, false);
                    assertThat(bytes(fixture, restored)).containsExactly(expected.getCopyOfBuffer());
                    restored.endInput();
                    restored.waitForTaskCompletion();
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    private static byte[] exchange(SharedAppendTopNFixture fixture) {
        return NativeExchangePlanSerializer.hash(fixture.top.input, new int[] {0}, 1, 1, true);
    }

    private static StreamTaskMailboxTestHarness<RowData> create(
            SharedAppendTopNFixture fixture, boolean rocks, boolean unaligned, TaskStateSnapshot state)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(fixture.top.input),
                fixture.top.output,
                fixture.plan(),
                List.of(id(1)),
                List.of(exchange(fixture)));
        return SharedKeyedChannelHarness.create(factory, fixture.top.output, new int[] {2}, rocks, unaligned, state);
    }

    private static void send(
            SharedAppendTopNFixture fixture,
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            List<FlinkStageMetricOracle> oracles,
            DataOutputSerializer expected,
            int seed,
            int phase,
            int channel,
            boolean capture)
            throws Exception {
        int count = 96;
        var rows = new ArrayList<GenericRowData>();
        var kinds = new RowKind[count];
        var present = new boolean[count];
        var timestamps = new long[count];
        var serializer = new RowDataSerializer(fixture.top.input);
        for (int key = 0; key < count; key++) {
            var row = GenericRowData.of(
                    key % 8 == 0 ? null : (long) (key % 8),
                    1L + phase * 2,
                    TimestampData.fromEpochMillis(key % 2),
                    StringData.fromString("é-" + seed + "-" + phase + "-" + key));
            rows.add(row);
            kinds[key] = RowKind.INSERT;
            present[key] = key % 3 != 0;
            timestamps[key] = phase == 1 ? Long.MIN_VALUE : seed * 1000L + phase;
            var copy = serializer.toBinaryRow(row).copy();
            List<StreamElement> events =
                    List.of(present[key] ? new StreamRecord<>(copy, timestamps[key]) : new StreamRecord<>(copy));
            for (var oracle : oracles) {
                for (var event : events) oracle.accept(event);
                events = oracle.drain();
            }
            for (var event : events) StageEventBytes.encode(fixture.top.output, event, expected);
        }
        try (var batch = ArrowRowDataBatch.transpose(rows, fixture.top.input, allocator)
                        .withEnvelope(kinds, present, timestamps);
                var envelope = ArrowExchangeBatch.withEnvelope(batch, fixture.top.input)) {
            for (var frame : ArrowExchangeCDataBridge.route(exchange(fixture), envelope.batch(), allocator, memory)) {
                if (capture) SharedChannelStateIO.capture(task, 0, channel, frame);
                task.processElement(new StreamRecord<>(frame), 0, channel);
            }
        }
    }

    private static byte[] bytes(SharedAppendTopNFixture fixture, StreamTaskMailboxTestHarness<RowData> task)
            throws Exception {
        var bytes = new DataOutputSerializer(128);
        for (var event : task.getOutput())
            if (event instanceof StreamRecord)
                StageEventBytes.encode(fixture.top.output, (StreamRecord<?>) event, bytes);
        return bytes.getCopyOfBuffer();
    }
}
