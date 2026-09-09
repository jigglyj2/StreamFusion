/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Restore both the original Flink DISTINCT handler and the common native runtime. */
class DistinctCountCheckpointTest {
    @ParameterizedTest
    @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,2", "true,2"})
    void signedMembershipAndFilteredCountsSurviveCanonicalAndCheckpointRestore(boolean rocks, int mode)
            throws Exception {
        var live = new ArrayList<GenericRowData>();
        OperatorSubtaskState reference;
        OperatorSubtaskState state;
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var oracle = DistinctCountFlinkOracle.create(rocks);
                    var source = DistinctCountRecoveryFixture.region(rocks, null, 1, 0)) {
                DistinctCountRecoveryFixture.input(
                        List.of(source), oracle, allocator, DistinctCountRecoveryFixture.changes(live, 0), 0);
                oracle.prepareSnapshotPreBarrier(1);
                reference = oracle.snapshot(1, 1);
                state = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 1);
                if (mode == 0 || !rocks) assertThat(state.getRawKeyedState()).isNotEmpty();
                else assertThat(state.getManagedKeyedState()).isNotEmpty();
            }
            try (var oracle = DistinctCountFlinkOracle.create(rocks, reference);
                    var target = DistinctCountRecoveryFixture.region(mode == 0 ? !rocks : rocks, state, 1, 0)) {
                DistinctCountRecoveryFixture.input(
                        List.of(target), oracle, allocator, DistinctCountRecoveryFixture.changes(live, 1), 1);
                DistinctCountRecoveryFixture.input(
                        List.of(target), oracle, allocator, DistinctCountRecoveryFixture.changes(live, 2), 2);
                assertThat(live).isEmpty();
            } finally {
                reference.discardState();
                state.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
