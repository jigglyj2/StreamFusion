/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSortLimit;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionRankSubclassAdmissionTest {
    @Test
    void limitsInheritRankStateAdmissionBeforeGraphReplacement() {
        var config = new Configuration();
        var type = RowType.of(new IntType(false));
        var source = new BatchExecTableSourceScan(config, null, type, "source");
        source.setInputEdges(List.of());
        for (var node : List.of(
                new StreamExecLimit(config, 1, 3, false, false, InputProperty.DEFAULT, type, "append limit"),
                new StreamExecLimit(config, 1, 3, true, true, InputProperty.DEFAULT, type, "retract limit"),
                new StreamExecSortLimit(
                        config,
                        SortSpec.builder().addField(0, true, false).build(),
                        1,
                        3,
                        RankProcessStrategy.APPEND_FAST_STRATEGY,
                        false,
                        InputProperty.DEFAULT,
                        type,
                        "sort limit"))) {
            var edge = ExecEdge.builder().source(source).target(node).build();
            node.setInputEdges(List.of(edge));
            var graph = new ExecNodeGraph(List.of(node));
            var reasons = new ArrayList<String>();
            StreamFusionArchitectureSupport.collect(graph, reasons, config);
            assertThat(reasons).anySatisfy(reason -> assertThat(reason)
                    .contains("root[0]/" + node.getClass().getSimpleName(), "rank persistent admission:"));
            assertThat(new StreamFusionExecGraphProcessor().process(graph, null))
                    .isSameAs(graph);
            assertThat(node.getInputEdges()).containsExactly(edge);
        }
    }
}
