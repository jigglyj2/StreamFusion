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
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.NativePlan;

class StreamFusionCalcChainPlanTest {
    @Test
    void serializesAdjacentCalcsAsOneNestedNativePlan() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        Expression value = StreamFusionIdentityCalcOperator.inputReference(
                0, StreamFusionIdentityCalcOperator.logicalType(rowType, 0));

        byte[] bytes = StreamFusionIdentityCalcOperator.createPlan(
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
}
