/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.proto.plan.v1.ArrayUnnest;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.UnnestCollection;

class StreamFusionCalcChainPlanTest {
    @Test
    void serializesAdjacentCalcsAsOneNestedNativePlan() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        Expression value = StreamFusionCalcPlan.inputReference(0, StreamFusionCalcPlan.logicalType(rowType, 0));

        byte[] bytes = StreamFusionCalcPlan.create(
                rowType, List.of(List.of(value), List.of(value)), Arrays.asList(null, null));

        NativePlan plan = NativePlan.parseFrom(bytes);
        Calc outer = plan.getRoot().getCalc();
        Calc inner = outer.getInput().getCalc();
        assertThat(inner.getInput().hasInput()).isTrue();
        assertThat(inner.getProjectionsCount()).isEqualTo(2);
        assertThat(outer.getProjectionsCount()).isEqualTo(2);
        assertThat(inner.getProjections(1).getInputReference().getIndex()).isEqualTo(1);
        assertThat(outer.getProjections(1).getInputReference().getIndex()).isEqualTo(1);
    }

    @Test
    void serializesCalcsOnBothSidesOfUnnestAsOneNativePlan() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        Expression value = StreamFusionCalcPlan.inputReference(0, StreamFusionCalcPlan.logicalType(rowType, 0));

        byte[] inputCalc =
                StreamFusionCalcPlan.create(rowType, List.of(List.of(value)), Arrays.asList((Expression) null));
        byte[] unnest = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setArrayUnnest(ArrayUnnest.newBuilder()
                                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                                .setArrayIndex(0)
                                .setCollection(UnnestCollection.UNNEST_COLLECTION_ARRAY)))
                .build()
                .toByteArray();
        byte[] output = StreamFusionCalcPlan.create(
                RowType.of(new IntType(false), new IntType()), List.of(List.of(value)), Arrays.asList((Expression)
                        null));
        byte[] bytes = StreamFusionNativeRegionTranslator.compose(List.of(inputCalc, unnest, output));

        NativePlan plan = NativePlan.parseFrom(bytes);
        Calc outputCalc = plan.getRoot().getCalc();
        Calc firstCalc = outputCalc.getInput().getArrayUnnest().getInput().getCalc();
        assertThat(firstCalc.getInput().hasInput()).isTrue();
        assertThat(outputCalc.getInput().hasArrayUnnest()).isTrue();
        assertThat(outputCalc.getProjections(1).getInputReference().getIndex()).isEqualTo(2);
    }
}
