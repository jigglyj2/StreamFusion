/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedWindowJoinFixture.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

class SharedWindowJoinChannelRecoveryTest {
    @ParameterizedTest
    @CsvSource({
        "false,false,false",
        "false,false,true",
        "false,true,false",
        "false,true,true",
        "true,false,false",
        "true,false,true",
        "true,true,false",
        "true,true,true"
    })
    void pendingWindowRestoresWithInflightArrowFramesExactlyOnce(boolean rocks, boolean unaligned, boolean keyed)
            throws Exception {
        var fixture = new SharedWindowJoinFixture(keyed, true);
        try (var oracle = fixture.join(rocks);
                var calc = fixture.calc();
                var allocator = new RootAllocator(64L << 20)) {
            var routing = new SharedChannelStateIO.RoutingMemory();
            TaskStateSnapshot checkpoint;
            var left = GenericRowData.of(7L, 200L, 5L, StringData.fromString("left"));
            var right = GenericRowData.of(7L, 200L, 3L, StringData.fromString("right"));
            oracle.accept(0, binary(left));
            oracle.accept(1, binary(right));
            try (var source = task(fixture, rocks, unaligned, null)) {
                send(fixture, source, allocator, routing, 0, left, false);
                var options = unaligned
                        ? CheckpointOptions.unaligned(
                                CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault())
                        : CheckpointOptions.alignedNoTimeout(
                                CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());
                var barrier = new CheckpointBarrier(1, 1, options);
                source.processEvent(barrier, 0);
                send(fixture, source, allocator, routing, 1, right, unaligned);
                source.processEvent(barrier, 1);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (source.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                    source.processAll();
                    Thread.sleep(5);
                }
                assertThat(source.getTaskStateManager().getReportedCheckpointId())
                        .isEqualTo(1);
                checkpoint = source.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                assertThat(checkpoint
                                .getSubtaskStateByOperatorID(SharedKeyedChannelHarness.REGION)
                                .getInputChannelState()
                                .isEmpty())
                        .isEqualTo(!unaligned);
                assertThat(bytes(source)).isEmpty();
                source.endInput();
                source.waitForTaskCompletion();
            }
            try (var restored = task(fixture, rocks, unaligned, checkpoint)) {
                restored.processAll();
                assertThat(bytes(restored)).isEmpty();
                // Another row after restore makes duplicate replay or premature close observable.
                var after = GenericRowData.of(7L, 200L, 4L, StringData.fromString("after"));
                oracle.accept(1, binary(after));
                send(fixture, restored, allocator, routing, 1, after, false);
                for (int port = 0; port < 2; port++) {
                    oracle.accept(port, new Watermark(199));
                    restored.processElement(new Watermark(199), port);
                }
                var expected = new DataOutputSerializer(128);
                for (var event : oracle.drain()) calc.accept(event);
                int records = 0;
                for (var event : calc.drain())
                    if (event instanceof StreamRecord) {
                        records++;
                        StageEventBytes.encode(OUTPUT, event, expected);
                    }
                assertThat(records).isEqualTo(2);
                assertThat(bytes(restored)).isEqualTo(expected.getCopyOfBuffer());
                restored.endInput();
                restored.waitForTaskCompletion();
            } finally {
                checkpoint.discardState();
            }
            assertThat(routing.available()).isEqualTo(routing.limit());
        }
    }

    private static StreamRecord<RowData> binary(RowData row) {
        return new StreamRecord<>(new RowDataSerializer(INPUT).toBinaryRow(row).copy());
    }

    private static byte[] exchange(SharedWindowJoinFixture fixture) {
        return fixture.keys.length == 0
                ? NativeExchangePlanSerializer.singleton(INPUT)
                : NativeExchangePlanSerializer.hash(INPUT, fixture.keys, 1, 1, true);
    }

    private static StreamTaskMailboxTestHarness<RowData> task(
            SharedWindowJoinFixture fixture, boolean rocks, boolean unaligned, TaskStateSnapshot restored)
            throws Exception {
        byte[] exchange = exchange(fixture);
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(INPUT, INPUT), OUTPUT, fixture.plan(), List.of(id(0)), List.of(exchange, exchange));
        return SharedKeyedChannelHarness.create(factory, OUTPUT, new int[] {1, 1}, rocks, unaligned, restored);
    }

    private static void send(
            SharedWindowJoinFixture fixture,
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            int port,
            GenericRowData row,
            boolean capture)
            throws Exception {
        try (var batch = ArrowRowDataBatch.transpose(List.of(row), INPUT, allocator);
                var envelope = ArrowExchangeBatch.withEnvelope(batch, INPUT, null)) {
            for (var frame : ArrowExchangeCDataBridge.route(exchange(fixture), envelope.batch(), allocator, memory)) {
                if (capture) SharedChannelStateIO.capture(task, port, 0, frame);
                task.processElement(new StreamRecord<>(frame), port);
            }
        }
    }

    private static byte[] bytes(StreamTaskMailboxTestHarness<RowData> task) throws Exception {
        var result = new DataOutputSerializer(128);
        for (var event : task.getOutput())
            if (event instanceof StreamRecord) StageEventBytes.encode(OUTPUT, (StreamRecord<?>) event, result);
        return result.getCopyOfBuffer();
    }
}
