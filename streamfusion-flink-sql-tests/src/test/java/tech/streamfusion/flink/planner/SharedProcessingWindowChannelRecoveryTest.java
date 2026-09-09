/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedProcessingWindowFixture.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;

/** Actual barriers and channel-state replay must sample the receiving task's restored clock. */
class SharedProcessingWindowChannelRecoveryTest {
    @ParameterizedTest(name = "rocks={0}, unaligned={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void replayUsesRestoredProcessingTimeAndEmitsEachRecordOnce(boolean rocks, boolean unaligned) throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            TaskStateSnapshot checkpoint = null;
            OperatorSubtaskState reference = null;
            try (var allocator = new RootAllocator(64L << 20)) {
                var memory = new SharedChannelStateIO.RoutingMemory();
                var inflight = rows(seed);
                var clock = new TestProcessingTimeService();
                clock.setCurrentTime(10001);
                try (var flink = oracle(rocks, null);
                        var task = create(rocks, unaligned, null, clock)) {
                    flink.setProcessingTime(10001);
                    input(flink, task, allocator, memory, 0, rows(seed + 1), false);
                    watermark(flink, task, 1337);
                    var location = CheckpointStorageLocationReference.getDefault();
                    var options = unaligned
                            ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                            : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                    var barrier = new CheckpointBarrier(1, 1, options);
                    if (unaligned) reference = snapshot(flink);
                    task.processEvent(barrier, 0, 0);
                    flink.setProcessingTime(10002);
                    clock.setCurrentTime(10002);
                    // Before channel 1's barrier, unaligned capture precedes state mutations.
                    input(flink, task, allocator, memory, 1, inflight, unaligned);
                    if (!unaligned) reference = snapshot(flink);
                    task.processEvent(barrier, 0, 1);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (task.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                        task.processAll();
                        Thread.sleep(5);
                    }
                    assertThat(task.getTaskStateManager().getReportedCheckpointId())
                            .isEqualTo(1);
                    checkpoint = task.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                    var state = checkpoint.getSubtaskStateByOperatorID(SharedKeyedChannelHarness.REGION);
                    assertThat(state.getInputChannelState().isEmpty()).isEqualTo(!unaligned);
                    assertThat(state.getManagedOperatorState()).isNotEmpty();
                    assertThat(task.getOutput().stream()
                                    .filter(event -> event instanceof CheckpointBarrier)
                                    .count())
                            .isEqualTo(1);
                    compare(flink, task);
                    task.endInput();
                    task.waitForTaskCompletion();
                }
                var restoredClock = new TestProcessingTimeService();
                restoredClock.setCurrentTime(20001);
                try (var flink = oracle(rocks, reference);
                        var task = create(rocks, unaligned, checkpoint, restoredClock)) {
                    // The initial records retain the absolute 19999 timer. Captured IPC contains
                    // no processing clock: replayed records now belong to the window ending 30000.
                    flink.setProcessingTime(20001);
                    if (unaligned) referenceRows(flink, inflight);
                    task.processAll();
                    restoredClock.setCurrentTime(20001);
                    task.processAll();
                    assertThat(task.getOutput().stream().anyMatch(event -> event instanceof StreamRecord<?>))
                            .isTrue();
                    compare(flink, task);
                    flink.setProcessingTime(20002);
                    restoredClock.setCurrentTime(20002);
                    input(flink, task, allocator, memory, 0, rows(seed + 2), false);
                    watermark(flink, task, 2337);
                    flink.setProcessingTime(29999);
                    restoredClock.setCurrentTime(29999);
                    task.processAll();
                    assertThat(task.getOutput().stream().anyMatch(event -> event instanceof StreamRecord<?>))
                            .isTrue();
                    compare(flink, task);
                    flink.setProcessingTime(30001);
                    restoredClock.setCurrentTime(30001);
                    input(flink, task, allocator, memory, 0, List.of(GenericRowData.of(7L, null)), false);
                    watermark(flink, task, Long.MAX_VALUE);
                    flink.getOperator().finish();
                    task.endInput();
                    task.waitForTaskCompletion();
                    compare(flink, task);
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
                assertThat(allocator.getAllocatedMemory()).isZero();
            } finally {
                if (reference != null) reference.discardState();
                if (checkpoint != null) checkpoint.discardState();
            }
        }
    }

    private static StreamTaskMailboxTestHarness<RowData> create(
            boolean rocks, boolean unaligned, TaskStateSnapshot state, TestProcessingTimeService clock)
            throws Exception {
        return SharedKeyedChannelHarness.create(
                factory(10000, exchange(), 1, 1), OUTPUT, new int[] {2}, rocks, unaligned, state, clock);
    }

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks, OperatorSubtaskState state) throws Exception {
        return ProcessingTimeWindowClockTest.oracle(rocks, 10000, state);
    }

    private static OperatorSubtaskState snapshot(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink) throws Exception {
        flink.prepareSnapshotPreBarrier(1);
        return flink.snapshot(1, 1);
    }

    private static byte[] exchange() {
        return NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 1, 1, true);
    }

    private static List<RowData> rows(int seed) {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        for (int i = 0; i < 137; i++) rows.add(GenericRowData.of(i % 7 == 0 ? null : (long) random.nextInt(9), null));
        return rows;
    }

    private static void referenceRows(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink, List<RowData> rows)
            throws Exception {
        var serializer = new RowDataSerializer(INPUT);
        for (var row : rows) flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row), 123));
    }

    private static void input(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            int channel,
            List<RowData> rows,
            boolean capture)
            throws Exception {
        referenceRows(flink, rows);
        for (int offset = 0; offset < rows.size(); offset += 31)
            try (var batch = ArrowRowDataBatch.transpose(
                            rows.subList(offset, Math.min(rows.size(), offset + 31)), INPUT, allocator);
                    var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT)) {
                for (var frame : ArrowExchangeCDataBridge.route(exchange(), envelope.batch(), allocator, memory)) {
                    if (capture) SharedChannelStateIO.capture(task, 0, channel, frame);
                    task.processElement(new StreamRecord<>(frame), 0, channel);
                }
            }
        // Preserve the checkpoint barrier until the caller checks its count.
        var barriers = new ArrayList<Object>();
        for (var event : task.getOutput()) if (event instanceof CheckpointBarrier) barriers.add(event);
        compare(flink, task);
        task.getOutput().addAll(barriers);
    }

    private static void watermark(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            StreamTaskMailboxTestHarness<RowData> task,
            long value)
            throws Exception {
        flink.processWatermark(new Watermark(value));
        task.processElement(new Watermark(value), 0, 0);
        task.processElement(new Watermark(value), 0, 1);
        compare(flink, task);
    }

    private static void compare(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            StreamTaskMailboxTestHarness<RowData> task)
            throws Exception {
        assertThat(bytes(task.getOutput())).containsExactly(bytes(flink.getOutput()));
    }

    private static byte[] bytes(Queue<Object> events) throws Exception {
        var output = new DataOutputSerializer(128);
        for (var event : events)
            if (event instanceof StreamRecord<?> || event instanceof Watermark)
                StageEventBytes.encode(OUTPUT, (StreamElement) event, output);
        events.clear();
        return WindowTimerEventBytes.canonical(OUTPUT, 3, output.getCopyOfBuffer());
    }
}
