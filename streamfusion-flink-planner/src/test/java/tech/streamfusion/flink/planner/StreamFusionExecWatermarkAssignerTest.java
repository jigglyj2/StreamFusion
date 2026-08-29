/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionExecWatermarkAssignerTest {
    @Test
    void replacesFlinksAssignerWithDistinctNodeThatRetainsItsInput() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new TimestampType(false, 3));
        RelDataTypeFactory typeFactory = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        RexInputRef watermarkExpression = new RexInputRef(0, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, rowType, "source");
        source.setInputEdges(List.of());
        StreamExecWatermarkAssigner watermark = new StreamExecWatermarkAssigner(
                configuration, watermarkExpression, 0, InputProperty.DEFAULT, rowType, "watermark");
        watermark.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(watermark).build()));

        ExecNodeGraph result =
                new StreamFusionExecGraphProcessor().process(new ExecNodeGraph(List.of(watermark)), null);

        ExecNode<?> replacement = result.getRootNodes().get(0);
        assertThat(replacement).isInstanceOf(StreamFusionExecWatermarkAssigner.class);
        assertThat(replacement.getInputEdges())
                .singleElement()
                .extracting(ExecEdge::getSource)
                .isEqualTo(source);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}
