/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SharedMiniBatchRescalingTest {
    @ParameterizedTest
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,2", "true,2"})
    void oneToTwoToOnePreservesPerSubtaskBundlesAndMetrics(boolean rocks, int mode) throws Exception {
        var live = new ArrayList<GenericRowData>();
        var snapshots = new ArrayList<MiniBatchRescalingFixture.Snapshots>();
        try (var allocator = new RootAllocator(64L << 20)) {
            MiniBatchRescalingFixture.Snapshots first;
            try (var source = new MiniBatchRescalingFixture(rocks, 1, 0, null)) {
                MiniBatchRescalingFixture.input(
                        List.of(source), allocator, SharedAggregateRescalingTest.changes(live, 0), 0);
                first = source.snapshot(mode, 100);
                snapshots.add(first);
            }
            boolean scaledRocks = mode == 0 ? !rocks : rocks;
            MiniBatchRescalingFixture.Snapshots second;
            try (var left = new MiniBatchRescalingFixture(scaledRocks, 2, 0, first.assign(1, 2, 0));
                    var right = new MiniBatchRescalingFixture(scaledRocks, 2, 1, first.assign(1, 2, 1))) {
                MiniBatchRescalingFixture.input(
                        List.of(left, right), allocator, SharedAggregateRescalingTest.changes(live, 1), 1);
                var leftState = left.snapshot(mode, 101);
                snapshots.add(leftState);
                var rightState = right.snapshot(mode, 101);
                snapshots.add(rightState);
                second = MiniBatchRescalingFixture.Snapshots.combine(leftState, rightState);
            }
            try (var target = new MiniBatchRescalingFixture(rocks, 1, 0, second.assign(2, 1, 0))) {
                MiniBatchRescalingFixture.input(
                        List.of(target), allocator, SharedAggregateRescalingTest.changes(live, 2), 2);
                target.finish();
            }
            assertThat(live).isEmpty();
            assertThat(allocator.getAllocatedMemory()).isZero();
        } finally {
            for (var state : snapshots) state.discard();
        }
    }
}
