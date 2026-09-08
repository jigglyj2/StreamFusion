/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Channel replay through global window -> Calc -> buffered local MAX, with both exits observable. */
class SharedAttachedMaxChannelRecoveryTest extends SharedWindowChannelRecoveryTest {
    private int outputBarriers;
    private final Queue<Object> sideOutput = new ArrayDeque<>();
    private SharedAttachedMaxFixture fixture;
    private OneInputStreamOperatorTestHarness<RowData, RowData> local;

    @Override
    protected AutoCloseable additionalOracle(boolean attached) throws Exception {
        fixture = new SharedAttachedMaxFixture(attached);
        local = LocalWindowFlinkOracle.create(fixture.flink, SharedAttachedMaxFixture.BUFFER_BYTES);
        return local;
    }

    @Override
    protected void beforeCheckpoint(long id) throws Exception {
        local.prepareSnapshotPreBarrier(id);
    }

    @Override
    protected StreamTaskMailboxTestHarness<RowData> create(
            boolean attached, boolean rocks, boolean unaligned, TaskStateSnapshot state) throws Exception {
        sideOutput.clear();
        outputBarriers = 0;
        var output = attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
        var exchange = NativeExchangePlanSerializer.hash(SharedSlicingWindowFixture.INPUT, new int[] {0}, 1, 1, true);
        var factory = StreamFusionNativeRegionOperatorFactory.shared(
                List.of(SharedSlicingWindowFixture.INPUT),
                List.of(output, SharedAttachedMaxFixture.PARTIAL),
                fixture.plan.toByteArray(),
                List.of(3L),
                List.of(exchange),
                fixture.resources());
        return SharedKeyedChannelHarness.create(
                factory, output, new int[] {2}, rocks, unaligned, state, sideOutput, SharedAttachedMaxFixture.PARTIAL);
    }

    @Override
    protected void compareAdditionalOutputs(boolean attached, List<StreamElement> events) throws Exception {
        for (var event : events) {
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                var projected = new StreamRecord<>(fixture.project((RowData) record.getValue()));
                if (record.hasTimestamp()) projected.setTimestamp(record.getTimestamp());
                local.processElement(projected);
            } else if (event instanceof Watermark) local.processWatermark((Watermark) event);
            else throw new AssertionError("Uncovered global window output " + event);
        }
        outputBarriers += sideOutput.stream()
                .filter(event -> event instanceof org.apache.flink.runtime.io.network.api.CheckpointBarrier)
                .count();
        var expected = new DataOutputSerializer(128);
        for (var event : local.getOutput()) StageEventBytes.encode(fixture.output, (StreamElement) event, expected);
        local.getOutput().clear();
        var actual = new DataOutputSerializer(128);
        for (var event : sideOutput) {
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                var row = (RowData) record.getValue();
                var bytes = row.getBinary(0);
                assertThat(bytes).hasSize(45);
                var partial = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                long count = partial.getLong(37);
                assertThat(partial.getLong(5)).isEqualTo(count);
                assertThat(row.getLong(1)).isEqualTo(row.getLong(2) - 6000);
                var decoded = GenericRowData.of(partial.getLong(20), count, row.getLong(2));
                decoded.setRowKind(row.getRowKind());
                StageEventBytes.row(fixture.output, decoded, record.hasTimestamp(), record.getTimestamp(), actual);
            } else if (event instanceof Watermark) StageEventBytes.encode(fixture.output, (Watermark) event, actual);
        }
        sideOutput.clear();
        assertThat(actual.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
    }

    @Override
    protected void verifyCheckpointOutputs() {
        org.assertj.core.api.Assertions.assertThat(outputBarriers).isEqualTo(1);
    }
}
