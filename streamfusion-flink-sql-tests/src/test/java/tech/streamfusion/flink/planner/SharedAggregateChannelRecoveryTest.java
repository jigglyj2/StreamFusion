/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

class SharedAggregateChannelRecoveryTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void restoredAggregateReplaysCapturedArrowChangelogExactlyOnce(boolean rocks, boolean unaligned) throws Exception {
        try (var oracle = oracle(rocks);
                var allocator = new RootAllocator(64L << 20)) {
            var memory = new SharedChannelStateIO.RoutingMemory();
            var expected = new DataOutputSerializer(128);
            TaskStateSnapshot checkpoint;
            try (var task = create(rocks, unaligned, null)) {
                var first = row(Long.MAX_VALUE, RowKind.INSERT);
                oracle.processElement(new StreamRecord<>(first));
                send(task, allocator, memory, 0, first, false);
                drain(oracle.getOutput(), expected);
                assertThat(bytes(task)).containsExactly(expected.getCopyOfBuffer());
                // Only post-checkpoint output may be emitted by the restored task.
                expected.clear();
                task.getOutput().clear();
                var location = CheckpointStorageLocationReference.getDefault();
                var options = unaligned
                        ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                        : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                var barrier = new CheckpointBarrier(1, 1, options);
                task.processEvent(barrier, 0, 0);
                var inflight = row(1L, RowKind.UPDATE_AFTER);
                oracle.processElement(new StreamRecord<>(inflight));
                send(task, allocator, memory, 1, inflight, unaligned);
                task.processEvent(barrier, 0, 1);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (task.getTaskStateManager().getReportedCheckpointId() != 1 && System.nanoTime() < deadline) {
                    task.processAll();
                    Thread.sleep(5);
                }
                assertThat(task.getTaskStateManager().getReportedCheckpointId()).isEqualTo(1);
                checkpoint = task.getTaskStateManager().getLastJobManagerTaskStateSnapshot();
                assertThat(checkpoint
                                .getSubtaskStateByOperatorID(SharedKeyedChannelHarness.REGION)
                                .getInputChannelState()
                                .isEmpty())
                        .isEqualTo(!unaligned);
                drain(oracle.getOutput(), expected);
                assertThat(bytes(task)).containsExactly(expected.getCopyOfBuffer());
                if (!unaligned) expected.clear();
                task.endInput();
                task.waitForTaskCompletion();
            }
            try (var restored = create(rocks, unaligned, checkpoint)) {
                restored.processAll();
                for (var value : List.of(row(Long.MAX_VALUE, RowKind.UPDATE_BEFORE), row(1L, RowKind.DELETE))) {
                    oracle.processElement(new StreamRecord<>(value));
                    send(restored, allocator, memory, 0, value, false);
                }
                drain(oracle.getOutput(), expected);
                assertThat(bytes(restored)).containsExactly(expected.getCopyOfBuffer());
                restored.endInput();
                restored.waitForTaskCompletion();
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    protected org.apache.flink.table.types.logical.RowType inputType() {
        return SharedAggregateFlinkOracle.INPUT;
    }

    protected org.apache.flink.table.types.logical.RowType outputType() {
        return SharedAggregateFlinkOracle.OUTPUT;
    }

    protected byte[] plan() {
        return SharedAggregateRegionParityTest.plan();
    }

    protected org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks) throws Exception {
        return SharedAggregateFlinkOracle.create(rocks);
    }

    protected GenericRowData row(long value, RowKind kind) {
        var row = GenericRowData.of(StringData.fromString("é-group"), value);
        row.setRowKind(kind);
        return row;
    }

    private byte[] exchange() {
        return NativeExchangePlanSerializer.hash(inputType(), new int[] {0}, 1, 1, true);
    }

    private StreamTaskMailboxTestHarness<RowData> create(boolean rocks, boolean unaligned, TaskStateSnapshot state)
            throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(inputType()), outputType(), plan(), List.of(3L), List.of(exchange()));
        return SharedKeyedChannelHarness.create(factory, outputType(), new int[] {2}, rocks, unaligned, state);
    }

    private void send(
            StreamTaskMailboxTestHarness<RowData> task,
            RootAllocator allocator,
            SharedChannelStateIO.RoutingMemory memory,
            int channel,
            GenericRowData row,
            boolean capture)
            throws Exception {
        try (var batch = ArrowRowDataBatch.transpose(List.of(row), inputType(), allocator)
                        .withRowKinds(new RowKind[] {row.getRowKind()});
                var envelope = ArrowExchangeBatch.withEnvelope(batch, inputType(), null)) {
            for (var frame : ArrowExchangeCDataBridge.route(exchange(), envelope.batch(), allocator, memory)) {
                if (capture) SharedChannelStateIO.capture(task, 0, channel, frame);
                task.processElement(new StreamRecord<>(frame), 0, channel);
            }
        }
    }

    private void drain(java.util.Queue<Object> events, DataOutputSerializer bytes) throws Exception {
        for (var event : events)
            if (event instanceof StreamRecord) StageEventBytes.encode(outputType(), (StreamRecord<?>) event, bytes);
        events.clear();
    }

    private byte[] bytes(StreamTaskMailboxTestHarness<RowData> task) throws Exception {
        var bytes = new DataOutputSerializer(128);
        for (var event : task.getOutput())
            if (event instanceof StreamRecord) StageEventBytes.encode(outputType(), (StreamRecord<?>) event, bytes);
        return bytes.getCopyOfBuffer();
    }
}
