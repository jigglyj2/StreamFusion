/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Reuses actual barrier/capture/replay mechanics; the third matrix flag selects composite strings. */
class DistinctWindowChannelRecoveryTest extends SharedWindowChannelRecoveryTest {
    private DistinctWindowFixture fixture;

    @Override
    protected KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean strings, boolean rocks)
            throws Exception {
        fixture = new DistinctWindowFixture(strings);
        return fixture.oracle(rocks, null);
    }

    @Override
    protected RowType output(boolean strings) {
        return fixture.output;
    }

    @Override
    protected RowType nativeInput() {
        return fixture.input;
    }

    @Override
    protected RowType flinkInput(boolean strings) {
        return fixture.flinkInput;
    }

    @Override
    protected RowData flinkPartial(boolean strings, RowData row) {
        return fixture.row((int) row.getLong(1), row.getLong(3), false);
    }

    @Override
    protected RowData nativePartial(boolean strings, RowData row) {
        return fixture.row((int) row.getLong(1), row.getLong(3), true);
    }

    @Override
    protected byte[] exchange() {
        return NativeExchangePlanSerializer.hash(
                fixture.input, fixture.strings ? new int[] {0, 1} : new int[] {0}, 1, 1, true);
    }

    @Override
    protected StreamTaskMailboxTestHarness<RowData> create(
            boolean strings, boolean rocks, boolean unaligned, TaskStateSnapshot state) throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(fixture.input), fixture.output, fixture.plan(), List.of(3L), List.of(exchange()));
        return SharedKeyedChannelHarness.create(factory, fixture.output, new int[] {2}, rocks, unaligned, state);
    }

    @Override
    protected void assertMainOutput(RowType type, java.util.Queue<Object> queue, byte[] expected) throws Exception {
        var actual = new DataOutputSerializer(128);
        for (var event : queue)
            if (event instanceof StreamRecord<?> || event instanceof Watermark)
                StageEventBytes.encode(type, (StreamElement) event, actual);
        queue.clear();
        assertThat(WindowTimerEventBytes.canonical(fixture.output, fixture.keys + 1, actual.getCopyOfBuffer()))
                .containsExactly(WindowTimerEventBytes.canonical(fixture.output, fixture.keys + 1, expected));
    }
}
