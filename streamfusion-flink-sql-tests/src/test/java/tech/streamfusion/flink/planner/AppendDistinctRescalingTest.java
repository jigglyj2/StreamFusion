/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Repartition presence bits through all 16 key groups, then revisit old members and filters. */
class AppendDistinctRescalingTest {
    @ParameterizedTest
    @CsvSource({
        "false,0,true",
        "true,0,true",
        "false,1,true",
        "true,1,true",
        "false,2,true",
        "true,2,true",
        "true,0,false",
        "true,1,false",
        "true,2,false"
    })
    void oneToTwoToOnePreservesPresenceAndIndependentFilterTransitions(boolean rocks, int mode, boolean incremental)
            throws Exception {
        try (var oracle = DistinctCountFlinkOracle.create(rocks, null, incremental, true);
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState first;
            try (var source = DistinctCountRecoveryFixture.region(rocks, null, 1, 0, incremental, true)) {
                DistinctCountRecoveryFixture.input(
                        List.of(source), oracle, allocator, DistinctCountRecoveryFixture.appendRows(3, false), 0);
                first = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 100);
            }
            var assigned0 = AbstractStreamOperatorTestHarness.repartitionOperatorState(first, 16, 1, 2, 0);
            var assigned1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(first, 16, 1, 2, 1);
            OperatorSubtaskState second;
            boolean scaledRocks = mode == 0 ? !rocks : rocks;
            try (var left = DistinctCountRecoveryFixture.region(scaledRocks, assigned0, 2, 0, incremental, true);
                    var right = DistinctCountRecoveryFixture.region(scaledRocks, assigned1, 2, 1, incremental, true)) {
                DistinctCountRecoveryFixture.input(
                        List.of(left, right), oracle, allocator, DistinctCountRecoveryFixture.appendRows(3, true), 1);
                second = AbstractStreamOperatorTestHarness.repackageState(
                        SharedWindowRuntimeRecoveryTest.snapshot(left, mode, 101),
                        SharedWindowRuntimeRecoveryTest.snapshot(right, mode, 101));
            }
            var assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(second, 16, 2, 1, 0);
            try (var target = DistinctCountRecoveryFixture.region(rocks, assigned, 1, 0, incremental, true)) {
                DistinctCountRecoveryFixture.input(
                        List.of(target), oracle, allocator, DistinctCountRecoveryFixture.appendRows(197, true), 2);
            } finally {
                first.discardState();
                second.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
