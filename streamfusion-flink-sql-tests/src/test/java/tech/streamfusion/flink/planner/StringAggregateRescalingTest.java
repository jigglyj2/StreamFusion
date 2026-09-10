/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Repartition variable-length extrema and filtered membership through all 16 composite key groups. */
class StringAggregateRescalingTest {
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
    void oneToTwoToOnePreservesStringExtremaAndFilteredMembers(boolean rocks, int mode, boolean incremental)
            throws Exception {
        try (var oracle = StringAggregateFlinkOracle.create(rocks, null, incremental);
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState first;
            try (var source = StringAggregateRecoveryFixture.region(rocks, null, 1, 0, incremental)) {
                StringAggregateRecoveryFixture.input(
                        List.of(source), oracle, allocator, StringAggregateRecoveryFixture.rows(3, 0), 0);
                first = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 100);
            }
            var assigned0 = AbstractStreamOperatorTestHarness.repartitionOperatorState(first, 16, 1, 2, 0);
            var assigned1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(first, 16, 1, 2, 1);
            OperatorSubtaskState second;
            boolean scaledRocks = mode == 0 ? !rocks : rocks;
            try (var left = StringAggregateRecoveryFixture.region(scaledRocks, assigned0, 2, 0, incremental);
                    var right = StringAggregateRecoveryFixture.region(scaledRocks, assigned1, 2, 1, incremental)) {
                StringAggregateRecoveryFixture.input(
                        List.of(left, right), oracle, allocator, StringAggregateRecoveryFixture.rows(3, 1), 1);
                second = AbstractStreamOperatorTestHarness.repackageState(
                        SharedWindowRuntimeRecoveryTest.snapshot(left, mode, 101),
                        SharedWindowRuntimeRecoveryTest.snapshot(right, mode, 101));
            }
            var assigned = AbstractStreamOperatorTestHarness.repartitionOperatorState(second, 16, 2, 1, 0);
            try (var target = StringAggregateRecoveryFixture.region(rocks, assigned, 1, 0, incremental)) {
                StringAggregateRecoveryFixture.input(
                        List.of(target), oracle, allocator, StringAggregateRecoveryFixture.rows(197, 2), 2);
            } finally {
                first.discardState();
                second.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
