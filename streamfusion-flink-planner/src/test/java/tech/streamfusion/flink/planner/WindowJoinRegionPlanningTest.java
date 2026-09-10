/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class WindowJoinRegionPlanningTest {
    @Test
    void windowJoinAndAdjacentCalcsBelongToOneNativeRegion() {
        var config = new Configuration();
        var type = RowType.of(new BigIntType());
        var leftSource = new StreamExecTableSourceScan(config, null, type, "left");
        var rightSource = new StreamExecTableSourceScan(config, null, type, "right");
        leftSource.setInputEdges(List.of());
        rightSource.setInputEdges(List.of());
        var left = unary(
                new StreamFusionExecCalc(config, List.of(), null, InputProperty.DEFAULT, type, "left calc"),
                leftSource);
        var right = unary(
                new StreamFusionExecCalc(config, List.of(), null, InputProperty.DEFAULT, type, "right calc"),
                rightSource);
        var join = new StreamFusionExecWindowJoin(
                config,
                new JoinSpec(FlinkJoinType.INNER, new int[0], new int[0], new boolean[0], null),
                null,
                null,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                type,
                "window join");
        join.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(join).build(),
                ExecEdge.builder().source(right).target(join).build()));
        var output = unary(
                new StreamFusionExecCalc(config, List.of(), null, InputProperty.DEFAULT, type, "output calc"), join);
        assertThat(StreamFusionStatelessRegion.stages(output)).containsExactly(left, right, join, output);
        assertThat(join.ownsNativeKeyedState()).isTrue();
        assertThat(join.ownsWindowBuffer()).isFalse();
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }
}
