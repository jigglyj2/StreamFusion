/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Flink's insert-only DISTINCT handler supplies the independent oracle for presence state. */
class AppendDistinctCheckpointTest {
    @ParameterizedTest
    @CsvSource({
        "false,0,true",
        "true,0,true",
        "false,1,true",
        "true,1,true",
        "false,2,true",
        "true,2,true",
        "true,1,false",
        "true,2,false"
    })
    void filteredPresenceSurvivesCanonicalAndPhysicalRestore(boolean rocks, int mode, boolean incremental)
            throws Exception {
        OperatorSubtaskState reference;
        OperatorSubtaskState state;
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var oracle = DistinctCountFlinkOracle.create(rocks, null, incremental, true);
                    var source = DistinctCountRecoveryFixture.region(rocks, null, 1, 0, incremental, true)) {
                DistinctCountRecoveryFixture.input(
                        List.of(source), oracle, allocator, DistinctCountRecoveryFixture.appendRows(3, false), 3);
                oracle.prepareSnapshotPreBarrier(1);
                reference = oracle.snapshot(1, 1);
                state = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 1);
            }
            try (var oracle = DistinctCountFlinkOracle.create(rocks, reference, incremental, true);
                    var target = DistinctCountRecoveryFixture.region(
                            mode == 0 ? !rocks : rocks, state, 1, 0, incremental, true)) {
                // Revisit old members with newly true filters, then introduce new values.
                DistinctCountRecoveryFixture.input(
                        List.of(target), oracle, allocator, DistinctCountRecoveryFixture.appendRows(3, true), 4);
                DistinctCountRecoveryFixture.input(
                        List.of(target), oracle, allocator, DistinctCountRecoveryFixture.appendRows(197, true), 5);
            } finally {
                reference.discardState();
                state.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
