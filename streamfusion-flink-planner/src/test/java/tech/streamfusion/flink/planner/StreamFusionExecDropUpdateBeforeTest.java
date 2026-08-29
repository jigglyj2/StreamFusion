/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExecDropUpdateBeforeTest {
    @Test
    void replacesFlinksChangelogFilterWithDistinctStreamFusionNode() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new IntType(false));
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, rowType, "source");
        source.setInputEdges(List.of());
        StreamExecDropUpdateBefore drop =
                new StreamExecDropUpdateBefore(configuration, InputProperty.DEFAULT, rowType, "drop update before");
        drop.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(drop).build()));

        ExecNodeGraph result = new StreamFusionExecGraphProcessor().process(new ExecNodeGraph(List.of(drop)), null);

        ExecNode<?> replacement = result.getRootNodes().get(0);
        assertThat(replacement).isInstanceOf(StreamFusionExecDropUpdateBefore.class);
        assertThat(replacement.getInputEdges())
                .singleElement()
                .extracting(ExecEdge::getSource)
                .isEqualTo(source);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}
