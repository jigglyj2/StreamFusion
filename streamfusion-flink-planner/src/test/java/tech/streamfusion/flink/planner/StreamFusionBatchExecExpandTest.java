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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionBatchExecExpandTest {
    @Test
    void representsBoundedExpandWithADistinctStreamFusionPhysicalNode() {
        RowType rowType = RowType.of(new IntType(false));
        RelDataTypeFactory typeFactory = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        RexInputRef input =
                new RexInputRef(0, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER));

        StreamFusionBatchExecExpand expand = new StreamFusionBatchExecExpand(
                new Configuration(), List.of(List.of(input), List.of(input)), InputProperty.DEFAULT, rowType, "expand");

        assertThat(expand).isInstanceOf(BatchExecNode.class);
        assertThat(expand).isInstanceOf(org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand.class);
        assertThat(expand.getDescription()).isEqualTo("expand");
    }
}
