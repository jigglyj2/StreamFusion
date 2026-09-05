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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionBatchExecCalcTest {
    @Test
    void convertsBoundedValuesAndCalcToDistinctBatchNodes() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new IntType(false));
        RexInputRef inputReference = inputReference();
        BatchExecValues source = new BatchExecValues(configuration, Collections.emptyList(), rowType, "source");
        BatchExecCalc calc =
                new BatchExecCalc(configuration, List.of(inputReference), null, InputProperty.DEFAULT, rowType, "calc");
        calc.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(calc).build()));

        ExecNode<?> converted = new StreamFusionExecGraphProcessor().convert(calc);

        assertThat(converted).isInstanceOf(StreamFusionBatchExecCalc.class);
        assertThat(converted).isInstanceOf(BatchExecNode.class);
        assertThat(converted.getInputEdges().get(0).getSource())
                .isInstanceOf(StreamFusionBatchExecValues.class)
                .isInstanceOf(BatchExecNode.class);
    }

    @Test
    void collectsAdjacentBoundedCalcsFromInputToOutput() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new IntType(false));
        RexInputRef inputReference = inputReference();
        BatchExecValues source = new BatchExecValues(configuration, Collections.emptyList(), rowType, "source");
        StreamFusionBatchExecCalc inner = calc(configuration, rowType, inputReference, "inner");
        StreamFusionBatchExecCalc outer = calc(configuration, rowType, inputReference, "outer");
        inner.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(inner).build()));
        outer.setInputEdges(
                List.of(ExecEdge.builder().source(inner).target(outer).build()));

        assertThat(StreamFusionBatchExecCalc.adjacentChain(outer)).containsExactly(inner, outer);
    }

    @Test
    void convertsBoundedUnionWithoutChangingItsBatchNodeContract() {
        Configuration configuration = new Configuration();
        RowType rowType = RowType.of(new IntType(false));
        BatchExecValues left = new BatchExecValues(configuration, Collections.emptyList(), rowType, "left");
        BatchExecValues right = new BatchExecValues(configuration, Collections.emptyList(), rowType, "right");
        BatchExecUnion union = new BatchExecUnion(
                configuration, List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), rowType, "union");
        union.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(union).build(),
                ExecEdge.builder().source(right).target(union).build()));

        ExecNode<?> converted = new StreamFusionExecGraphProcessor().convert(union);

        assertThat(converted).isInstanceOf(StreamFusionBatchExecUnion.class).isInstanceOf(BatchExecNode.class);
        assertThat(converted.getInputEdges())
                .extracting(ExecEdge::getSource)
                .allMatch(StreamFusionBatchExecValues.class::isInstance);
    }

    private RexInputRef inputReference() {
        RelDataTypeFactory typeFactory = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        return new RexInputRef(0, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER));
    }

    private static StreamFusionBatchExecCalc calc(
            Configuration configuration, RowType rowType, RexInputRef projection, String description) {
        return new StreamFusionBatchExecCalc(
                configuration, List.of(projection), null, InputProperty.DEFAULT, rowType, description);
    }
}
