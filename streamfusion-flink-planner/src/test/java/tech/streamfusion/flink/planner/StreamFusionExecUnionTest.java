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

import java.util.Collections;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExecUnionTest {
    @Test
    void replacesUnionAllWithDistinctStreamFusionPhysicalNode() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new IntType(false));
        StreamExecValues left = values(configuration, rowType, "left");
        StreamExecValues right = values(configuration, rowType, "right");
        StreamExecUnion union = new StreamExecUnion(
                configuration, List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), rowType, "union all");
        union.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(union).build(),
                ExecEdge.builder().source(right).target(union).build()));

        ExecNodeGraph result = new StreamFusionExecGraphProcessor().process(new ExecNodeGraph(List.of(union)), null);

        assertThat(result.getRootNodes()).singleElement().isInstanceOf(StreamFusionExecUnion.class);
        assertThat(result.getRootNodes().get(0).getInputEdges())
                .extracting(ExecEdge::getSource)
                .containsExactly(left, right);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static StreamExecValues values(Configuration configuration, RowType rowType, String description) {
        StreamExecValues values = new StreamExecValues(configuration, Collections.emptyList(), rowType, description);
        values.setInputEdges(Collections.emptyList());
        return values;
    }
}
