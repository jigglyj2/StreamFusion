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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExecLimitTest {
    @Test
    void replacesFlinkLimitWithTheSharedNativeRankNode() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(
                new LogicalType[] {new BigIntType(false), new BigIntType(true)}, new String[] {"id", "value"});
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, rowType, "source");
        source.setInputEdges(List.of());
        StreamExecLimit limit = new StreamExecLimit(
                configuration, 2, 7, true, true, InputProperty.DEFAULT, rowType, "LIMIT 5 OFFSET 2");
        limit.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(limit).build()));

        ExecNode<?> replacement = new StreamFusionExecGraphProcessor().convert(limit);

        assertThat(replacement).isInstanceOf(StreamFusionExecRank.class);
        assertThat(replacement.getInputEdges())
                .singleElement()
                .extracting(ExecEdge::getSource)
                .isEqualTo(source);
    }
}
