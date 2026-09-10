/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Generated string extrema and DISTINCT state survive canonical and physical checkpoints. */
class StringAggregateCheckpointTest {
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
    void stringExtremaSurviveCanonicalAndPhysicalRestore(boolean rocks, int mode, boolean incremental)
            throws Exception {
        OperatorSubtaskState reference;
        OperatorSubtaskState state;
        try (var allocator = new RootAllocator(64L << 20)) {
            try (var oracle = StringAggregateFlinkOracle.create(rocks, null, incremental);
                    var source = StringAggregateRecoveryFixture.region(rocks, null, 1, 0, incremental)) {
                StringAggregateRecoveryFixture.input(
                        List.of(source), oracle, allocator, StringAggregateRecoveryFixture.rows(3, 0), 3);
                oracle.prepareSnapshotPreBarrier(1);
                reference = oracle.snapshot(1, 1);
                state = SharedWindowRuntimeRecoveryTest.snapshot(source, mode, 1);
            }
            try (var oracle = StringAggregateFlinkOracle.create(rocks, reference, incremental);
                    var target = StringAggregateRecoveryFixture.region(
                            mode == 0 ? !rocks : rocks, state, 1, 0, incremental)) {
                // Revisit old members with newly true filters, then introduce new values.
                StringAggregateRecoveryFixture.input(
                        List.of(target), oracle, allocator, StringAggregateRecoveryFixture.rows(3, 1), 4);
                StringAggregateRecoveryFixture.input(
                        List.of(target), oracle, allocator, StringAggregateRecoveryFixture.rows(197, 2), 5);
            } finally {
                reference.discardState();
                state.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
