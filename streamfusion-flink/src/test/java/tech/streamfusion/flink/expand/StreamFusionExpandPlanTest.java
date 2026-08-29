/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.expand;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.NativePlan;

class StreamFusionExpandPlanTest {
    @Test
    void preservesProjectionAlternativesInTheNativePlan() throws Exception {
        Expression first = literal(1);
        Expression second = literal(2);

        NativePlan plan =
                NativePlan.parseFrom(StreamFusionExpandOperator.createPlan(List.of(List.of(first), List.of(second))));

        assertThat(plan.getRoot().getExpand().getProjectionsList())
                .extracting(projection ->
                        projection.getExpressions(0).getIntegerLiteral().getValue())
                .containsExactly(1, 2);
    }

    private static Expression literal(int value) {
        return Expression.newBuilder()
                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(value))
                .build();
    }
}
