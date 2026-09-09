/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Actual network barriers, channel-state writer/reader and Arrow IPC replay after a window clock restore. */
class SharedWindowChannelRecoveryTest {
    protected boolean tumbling() {
        return false;
    }

    private int outputBarriers;

    @ParameterizedTest
    @CsvSource({
        "false,false,false",
        "false,true,false",
        "true,false,false",
        "true,true,false",
        "false,false,true",
        "false,true,true",
        "true,false,true",
        "true,true,true"
    })
    void inflightPartialsReplayOnceWithTheRestoredWindowClock(boolean rocks, boolean unaligned, boolean attached)
            throws Exception {
        for (int seed = 0; seed < 3; seed++)
            try (var oracle = oracle(attached, rocks);
                    var allocator = new RootAllocator(64L << 20);
                    var additional = additionalOracle(attached)) {
                var memory = new SharedChannelStateIO.RoutingMemory();
                TaskStateSnapshot checkpoint;
                outputBarriers = 0;
                try (var task = create(attached, rocks, unaligned, null)) {
                    input(attached, oracle, task, allocator, memory, 0, initialRows(), false);
                    watermark(attached, oracle, task, initialWatermark());
                    var location = CheckpointStorageLocationReference.getDefault();
                    var options = unaligned
                            ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                            : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                    var barrier = new CheckpointBarrier(1, 1, options);
                    if (unaligned) beforeCheckpoint(1);
                    task.processEvent(barrier, 0, 0);
                    // The second channel has not delivered its barrier. An unaligned snapshot
                    // contains these IPC frames in channel state, before their keyed mutations.
                    var inflight = inflightRows(seed);
                    input(attached, oracle, task, allocator, memory, 1, inflight, unaligned);
                    if (!unaligned) beforeCheckpoint(1);
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
                    compare(attached, oracle, task);
                    assertThat(outputBarriers).isEqualTo(1);
                    verifyCheckpointOutputs();
                    task.endInput();
                    task.waitForTaskCompletion();
                }
                try (var restored = create(attached, rocks, unaligned, checkpoint)) {
                    restored.processAll();
                    compare(attached, oracle, restored);
                    watermark(attached, oracle, restored, 999);
                    input(attached, oracle, restored, allocator, memory, 0, restoredRows(), false);
                    for (long mark : finalWatermarks()) watermark(attached, oracle, restored, mark);
                    restored.endInput();
                    restored.waitForTaskCompletion();
                } finally {
                    checkpoint.discardState();
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
            }
    }

    protected List<RowData> initialRows() {
        return List.of(partial(2, 2000), partial(5, 4000));
    }

    protected long initialWatermark() {
        return 1999;
    }

    protected List<RowData> inflightRows(int seed) {
        var rows = new ArrayList<RowData>();
        rows.add(partial(99, -2000)); // fully late even for shared HOP
        rows.add(partial(7, 2000)); // late only for the attached window
        var random = new Random(seed);
        for (int row = 0; row < 31; row++) rows.add(partial(random.nextInt(99) + 1, (random.nextInt(7) - 1) * 2000L));
        return rows;
    }

    protected List<RowData> restoredRows() {
        return List.of(partial(13, 4000));
    }

    protected long[] finalWatermarks() {
        return new long[] {3999, 5999, 7999, Long.MAX_VALUE};
    }

    private static RowData partial(long value, long end) {
        return GenericRowData.of(1L, value, 1L, end);
    }

    protected RowType output(boolean attached) {
        return attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
    }

    protected byte[] exchange() {
        return NativeExchangePlanSerializer.hash(SharedSlicingWindowFixture.INPUT, new int[] {0}, 1, 1, true);
    }

    protected StreamTaskMailboxTestHarness<RowData> create(
            boolean attached, boolean rocks, boolean unaligned, TaskStateSnapshot state) throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(SharedSlicingWindowFixture.INPUT),
                output(attached),
                attached ? AttachedSlicingWindowFixture.plan(true) : SharedSlicingWindowFixture.plan(tumbling()),
                List.of(3L),
                List.of(exchange()));
        return SharedKeyedChannelHarness.create(factory, output(attached), new int[] {2}, rocks, unaligned, state);
    }

    private void input(
            boolean attached,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            int channel,
            List<RowData> rows,
            boolean capture)
            throws Exception {
        var serializer = new RowDataSerializer(flinkInput(attached));
        var nativeRows = new ArrayList<RowData>();
        for (var row : rows) {
            var flinkRow = flinkPartial(attached, row);
            oracle.processElement(new StreamRecord<>(serializer.toBinaryRow(flinkRow), 123));
            nativeRows.add(nativePartial(attached, row));
        }
        try (var batch = ArrowRowDataBatch.transpose(nativeRows, nativeInput(), allocator);
                var envelope = ArrowExchangeBatch.withEnvelope(batch, nativeInput(), null)) {
            for (var frame : ArrowExchangeCDataBridge.route(exchange(), envelope.batch(), allocator, memory)) {
                if (capture) SharedChannelStateIO.capture(task, 0, channel, frame);
                task.processElement(new StreamRecord<>(frame), 0, channel);
            }
        }
        compare(attached, oracle, task);
    }

    private void watermark(
            boolean attached,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            StreamTaskMailboxTestHarness<RowData> task,
            long value)
            throws Exception {
        oracle.processWatermark(new Watermark(value));
        task.processElement(new Watermark(value), 0, 0);
        task.processElement(new Watermark(value), 0, 1);
        compare(attached, oracle, task);
    }

    private void compare(
            boolean attached,
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle,
            StreamTaskMailboxTestHarness<RowData> task)
            throws Exception {
        var events = new ArrayList<StreamElement>();
        var expected = new DataOutputSerializer(128);
        for (var event : oracle.getOutput()) {
            events.add((StreamElement) event);
            StageEventBytes.encode(output(attached), (StreamElement) event, expected);
        }
        oracle.getOutput().clear();
        compareAdditionalOutputs(attached, events);
        outputBarriers += task.getOutput().stream()
                .filter(event -> event instanceof CheckpointBarrier)
                .count();
        assertMainOutput(output(attached), task.getOutput(), expected.getCopyOfBuffer());
    }

    protected KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean attached, boolean rocks)
            throws Exception {
        return attached
                ? GlobalWindowFlinkOracle.create(
                        SlicingWindowFlinkPlan.stage("GlobalWindowAggregate", AttachedSlicingWindowFixture.sql(true)),
                        rocks,
                        null)
                : GlobalWindowFlinkOracle.create(rocks, null, tumbling());
    }

    protected RowType nativeInput() {
        return SharedSlicingWindowFixture.INPUT;
    }

    protected RowType flinkInput(boolean attached) {
        return attached ? AttachedSlicingWindowFixture.FLINK_INPUT : SharedSlicingWindowFixture.FLINK_INPUT;
    }

    protected RowData flinkPartial(boolean attached, RowData row) {
        return attached ? row : GenericRowData.of(row.getLong(0), row.getLong(1), row.getLong(3));
    }

    protected RowData nativePartial(boolean attached, RowData row) {
        return GenericRowData.of(
                row.getLong(0),
                attached
                        ? AttachedSlicingWindowFixture.partial(row.getLong(1), row.getLong(2))
                        : SharedSlicingWindowFixture.count(row.getLong(1)),
                row.getLong(3) - (attached ? 6000 : 2000),
                row.getLong(3));
    }

    protected void assertMainOutput(RowType type, java.util.Queue<Object> queue, byte[] expected) throws Exception {
        assertOutput(type, queue, expected);
    }

    protected static void assertOutput(RowType type, java.util.Queue<Object> queue, byte[] expected) throws Exception {
        var actual = new DataOutputSerializer(128);
        for (var event : queue)
            if (event instanceof StreamRecord<?> || event instanceof Watermark)
                StageEventBytes.encode(type, (StreamElement) event, actual);
        queue.clear();
        assertThat(actual.getCopyOfBuffer()).containsExactly(expected);
    }

    protected void compareAdditionalOutputs(boolean attached, List<StreamElement> events) throws Exception {}

    protected AutoCloseable additionalOracle(boolean attached) throws Exception {
        return () -> {};
    }

    protected void beforeCheckpoint(long id) throws Exception {}

    protected void verifyCheckpointOutputs() {}
}
