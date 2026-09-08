/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.SharedBinaryJoinMetricFixture.id;

import java.util.List;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

final class SharedBinaryJoinChannelHarness {
    static final OperatorID REGION = SharedKeyedChannelHarness.REGION;

    static byte[] exchange(SharedBinaryJoinMetricFixture fixture) {
        // StreamMockEnvironment owns one key group; rescaling has a separate harness.
        return NativeExchangePlanSerializer.hash(fixture.input, new int[] {0}, 1, 1, true);
    }

    static StreamTaskMailboxTestHarness<RowData> create(
            SharedBinaryJoinMetricFixture fixture, boolean rocks, boolean unaligned, TaskStateSnapshot restore)
            throws Exception {
        byte[] exchange = exchange(fixture);
        var factory = new StreamFusionNativeRegionOperatorFactory(
                List.of(fixture.input, fixture.input),
                fixture.output,
                fixture.plan(),
                List.of(id(0)),
                List.of(exchange, exchange));
        return SharedKeyedChannelHarness.create(factory, fixture.output, new int[] {1, 1}, rocks, unaligned, restore);
    }
}
