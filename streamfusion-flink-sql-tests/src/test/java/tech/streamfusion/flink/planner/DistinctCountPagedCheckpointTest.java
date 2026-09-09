/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** One large aggregate group spans multiple restore pages, including shared filtered membership. */
class DistinctCountPagedCheckpointTest {
    @ParameterizedTest
    @CsvSource({"false,1", "false,2", "true,1", "true,2"})
    void fullAndIncrementalFileRestorePreserveAGroupLargerThanOnePage(boolean incremental, int mode) throws Exception {
        var input = new ArrayList<RowData>();
        var random = new Random(917);
        for (int index = 0; index < 8192; index++) {
            input.add(GenericRowData.of(StringData.fromString("large-é"), (long) index, random.nextBoolean()));
        }
        var after = new ArrayList<RowData>();
        for (int index = 0; index < input.size(); index++) {
            var row = (GenericRowData) input.get(index);
            var retract = GenericRowData.of(row.getField(0), row.getField(1), row.getField(2));
            retract.setRowKind(index % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
            after.add(retract);
        }
        // Recreate the deleted group after all restored memberships have been removed.
        after.add(GenericRowData.of(StringData.fromString("large-é"), 23L, true));
        OperatorSubtaskState nativeState;
        OperatorSubtaskState flinkState;
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var oracle = DistinctCountFlinkOracle.create(true, null, incremental);
                    var source = DistinctCountRecoveryFixture.region(true, null, 1, 0, incremental)) {
                // Phase 3 allows a hot key rather than the fixture's all-key-group initialization.
                DistinctCountRecoveryFixture.input(List.of(source), oracle, allocator, input, 3);
                oracle.prepareSnapshotPreBarrier(1);
                flinkState = oracle.snapshot(1, 1);
                nativeState = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 1);
                assertThat(nativeState.getRawKeyedState()).isEmpty();
                assertThat(nativeState.getManagedKeyedState()).isNotEmpty();
            }
            try (var oracle = DistinctCountFlinkOracle.create(true, flinkState, incremental);
                    var target = DistinctCountRecoveryFixture.region(true, nativeState, 1, 0, incremental)) {
                DistinctCountRecoveryFixture.input(List.of(target), oracle, allocator, after, 4);
            } finally {
                flinkState.discardState();
                nativeState.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
