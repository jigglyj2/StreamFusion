/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class StreamFusionBatchExecSortMergeJoinTest {
    @Test
    void retainsSortMergeIdentityWhileUsingTheNativeBoundedJoinContract() throws Exception {
        Configuration configuration = new Configuration();
        RowType inputType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType()}, new String[] {"id", "payload"});
        RowType outputType = RowType.of(
                new LogicalType[] {new IntType(), new VarCharType(), new IntType(), new VarCharType()},
                new String[] {"left_id", "left_payload", "right_id", "right_payload"});
        BatchExecTableSourceScan left = source(configuration, inputType, "left");
        BatchExecTableSourceScan right = source(configuration, inputType, "right");
        for (FlinkJoinType joinType : List.of(
                FlinkJoinType.INNER,
                FlinkJoinType.LEFT,
                FlinkJoinType.RIGHT,
                FlinkJoinType.FULL,
                FlinkJoinType.SEMI,
                FlinkJoinType.ANTI)) {
            BatchExecSortMergeJoin join = new BatchExecSortMergeJoin(
                    configuration,
                    joinType,
                    new int[] {0},
                    new int[] {0},
                    new boolean[] {true},
                    null,
                    16,
                    16,
                    2,
                    2,
                    true,
                    InputProperty.DEFAULT,
                    InputProperty.DEFAULT,
                    outputType,
                    true,
                    "sort merge join");
            join.setInputEdges(List.of(
                    ExecEdge.builder().source(left).target(join).build(),
                    ExecEdge.builder().source(right).target(join).build()));

            ExecNode<?> replacement = new StreamFusionExecGraphProcessor().convert(join);

            assertThat(replacement).isExactlyInstanceOf(StreamFusionBatchExecSortMergeJoin.class);
            assertThat(replacement.getDescription()).isEqualTo("StreamFusionBatchSortMergeJoin");
            assertThat(replacement.getInputEdges())
                    .extracting(ExecEdge::getSource)
                    .containsExactly(left, right);
            Field joinSpecField = StreamFusionBatchExecHashJoin.class.getDeclaredField("joinSpec");
            joinSpecField.setAccessible(true);
            JoinSpec joinSpec = (JoinSpec) joinSpecField.get(replacement);
            assertThat(joinSpec.getJoinType()).isEqualTo(joinType);
            assertThat(joinSpec.getLeftKeys()).containsExactly(0);
            assertThat(joinSpec.getRightKeys()).containsExactly(0);
            assertThat(joinSpec.getFilterNulls()).containsExactly(true);
        }
    }

    private static BatchExecTableSourceScan source(Configuration configuration, RowType type, String name) {
        BatchExecTableSourceScan source = new BatchExecTableSourceScan(configuration, null, type, name);
        source.setInputEdges(List.of());
        return source;
    }
}
