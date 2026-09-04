/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionExecTemporalJoinTest {
    @Test
    void replacesFlinkEventTimeTemporalJoinWithADistinctNativeNode() {
        Configuration configuration = new Configuration();
        RowType inputType = RowType.of(
                new LogicalType[] {new BigIntType(false), new TimestampType(false, TimestampKind.ROWTIME, 3)},
                new String[] {"id", "event_time"});
        RowType outputType = RowType.of(new LogicalType[] {
            new BigIntType(false),
            new TimestampType(false, TimestampKind.ROWTIME, 3),
            new BigIntType(true),
            new TimestampType(true, 3)
        });
        StreamExecTableSourceScan left = source(configuration, inputType, "left");
        StreamExecTableSourceScan right = source(configuration, inputType, "right");
        StreamExecTemporalJoin join = new StreamExecTemporalJoin(
                configuration,
                new JoinSpec(FlinkJoinType.LEFT, new int[] {0}, new int[] {0}, new boolean[] {true}, null),
                false,
                1,
                1,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                outputType,
                "temporal join");
        join.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(join).build(),
                ExecEdge.builder().source(right).target(join).build()));

        ExecNode<?> replacement = new StreamFusionExecGraphProcessor().convert(join);

        assertThat(replacement).isInstanceOf(StreamFusionExecTemporalJoin.class);
        assertThat(replacement.getInputEdges()).extracting(ExecEdge::getSource).containsExactly(left, right);
    }

    private static StreamExecTableSourceScan source(Configuration configuration, RowType type, String name) {
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, type, name);
        source.setInputEdges(List.of());
        return source;
    }
}
