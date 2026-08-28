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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexInputRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExecCalcTest {
    @Test
    void collectsAdjacentCalcsFromInputToOutput() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new IntType(false));
        RelDataTypeFactory typeFactory = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        RexInputRef inputReference =
                new RexInputRef(0, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER));
        StreamExecValues source = new StreamExecValues(configuration, Collections.emptyList(), rowType, "source");
        StreamFusionExecCalc inner = calc(configuration, rowType, inputReference, "inner");
        StreamFusionExecCalc outer = calc(configuration, rowType, inputReference, "outer");
        inner.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(inner).build()));
        outer.setInputEdges(
                List.of(ExecEdge.builder().source(inner).target(outer).build()));

        assertThat(StreamFusionExecCalc.adjacentChain(outer)).containsExactly(inner, outer);
    }

    private static StreamFusionExecCalc calc(
            Configuration configuration, RowType rowType, RexInputRef projection, String description) {
        return new StreamFusionExecCalc(
                configuration, List.of(projection), null, InputProperty.DEFAULT, rowType, description);
    }
}
