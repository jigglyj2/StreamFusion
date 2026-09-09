/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.SharedSessionWindowFixture.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Session-specific arrivals through the common real barrier/channel-state replay matrix. */
class SharedSessionWindowChannelRecoveryTest extends SharedWindowChannelRecoveryTest {
    @Override
    @ParameterizedTest(name = "rocks={0}, unaligned={1}")
    @CsvSource({"false,false,false", "false,true,false", "true,false,false", "true,true,false"})
    void inflightPartialsReplayOnceWithTheRestoredWindowClock(boolean rocks, boolean unaligned, boolean attached)
            throws Exception {
        super.inflightPartialsReplayOnceWithTheRestoredWindowClock(rocks, unaligned, attached);
    }

    @Override
    protected List<RowData> initialRows() {
        return List.of(row(10000), row(40000));
    }

    @Override
    protected long initialWatermark() {
        return 15000;
    }

    @Override
    protected List<RowData> inflightRows(int seed) {
        var rows = new ArrayList<RowData>();
        rows.add(row(-8000)); // isolated and expired; a reset clock would wrongly accept it
        rows.add(row(0)); // own window expired, but inclusive contact merges into a live session
        rows.add(row(20000));
        rows.add(row(30000)); // bridges the two retained namespaces
        var random = new Random(seed);
        for (int i = 0; i < 31; i++) rows.add(row((random.nextInt(71) - 20) * 1000L));
        return rows;
    }

    @Override
    protected List<RowData> restoredRows() {
        return List.of(row(5000), row(70000));
    }

    @Override
    protected long[] finalWatermarks() {
        return new long[] {19999, 49999, 79999, Long.MAX_VALUE};
    }

    @Override
    protected RowType output(boolean attached) {
        return OUTPUT;
    }

    @Override
    protected RowType nativeInput() {
        return INPUT;
    }

    @Override
    protected RowType flinkInput(boolean attached) {
        return INPUT;
    }

    @Override
    protected RowData flinkPartial(boolean attached, RowData row) {
        return row;
    }

    @Override
    protected RowData nativePartial(boolean attached, RowData row) {
        return row;
    }

    @Override
    protected byte[] exchange() {
        return NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 1, 1, true);
    }

    @Override
    protected KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean attached, boolean rocks)
            throws Exception {
        return GlobalWindowFlinkOracle.create(SlicingWindowFlinkPlan.stage("WindowAggregate", SQL), rocks, null);
    }

    @Override
    protected StreamTaskMailboxTestHarness<RowData> create(
            boolean attached, boolean rocks, boolean unaligned, TaskStateSnapshot state) throws Exception {
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(INPUT), OUTPUT, plan(), List.of(3L), List.of(exchange()));
        return SharedKeyedChannelHarness.create(factory, OUTPUT, new int[] {2}, rocks, unaligned, state);
    }
}
