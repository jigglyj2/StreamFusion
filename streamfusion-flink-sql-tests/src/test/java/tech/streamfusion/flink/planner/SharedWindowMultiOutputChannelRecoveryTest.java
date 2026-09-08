/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;

/** Replays real input channels into one shared owner and verifies both Flink output branches. */
class SharedWindowMultiOutputChannelRecoveryTest extends SharedWindowChannelRecoveryTest {
    private int outputBarriers;
    private final Queue<Object> sideOutput = new ArrayDeque<>();

    @Override
    protected StreamTaskMailboxTestHarness<RowData> create(
            boolean attached, boolean rocks, boolean unaligned, TaskStateSnapshot state) throws Exception {
        sideOutput.clear();
        outputBarriers = 0;
        var output = attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
        var exchange = NativeExchangePlanSerializer.hash(SharedSlicingWindowFixture.INPUT, new int[] {0}, 1, 1, true);
        var factory = StreamFusionNativeRegionOperatorFactory.shared(
                List.of(SharedSlicingWindowFixture.INPUT),
                List.of(output, output),
                SharedWindowMultiOutputFixture.plan(attached).toByteArray(),
                List.of(3L),
                List.of(exchange),
                NativeLocalWindowResources.NONE);
        return SharedKeyedChannelHarness.create(
                factory, output, new int[] {2}, rocks, unaligned, state, sideOutput, output);
    }

    @Override
    protected void compareAdditionalOutputs(
            boolean attached, List<org.apache.flink.streaming.runtime.streamrecord.StreamElement> events)
            throws Exception {
        var output = attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
        outputBarriers += sideOutput.stream()
                .filter(event -> event instanceof org.apache.flink.runtime.io.network.api.CheckpointBarrier)
                .count();
        var expected = new org.apache.flink.core.memory.DataOutputSerializer(128);
        for (var event : events) StageEventBytes.encode(output, event, expected);
        assertOutput(output, sideOutput, expected.getCopyOfBuffer());
    }

    @Override
    protected void verifyCheckpointOutputs() {
        org.assertj.core.api.Assertions.assertThat(outputBarriers).isEqualTo(1);
    }
}
