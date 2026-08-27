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

import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.BooleanOperator;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;

class StreamFusionBooleanConditionTest {
    @Test
    void preservesSqlThreeValuedBooleanLogic() {
        GenericRowData nullRow = GenericRowData.of((Object) null);
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        StreamFusionCondition unknown =
                new StreamFusionIntComparison(0, 2, ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL, true);
        StreamFusionCondition trueCondition = new StreamFusionNullCondition(0, false, reference);
        StreamFusionCondition falseCondition = new StreamFusionNullCondition(0, true, reference);

        assertThat(StreamFusionBooleanCondition.binary(unknown, falseCondition, BooleanOperator.BOOLEAN_OPERATOR_AND)
                        .evaluate(nullRow))
                .isFalse();
        assertThat(StreamFusionBooleanCondition.binary(unknown, trueCondition, BooleanOperator.BOOLEAN_OPERATOR_AND)
                        .evaluate(nullRow))
                .isNull();
        assertThat(StreamFusionBooleanCondition.binary(unknown, trueCondition, BooleanOperator.BOOLEAN_OPERATOR_OR)
                        .evaluate(nullRow))
                .isTrue();
        assertThat(StreamFusionBooleanCondition.binary(unknown, falseCondition, BooleanOperator.BOOLEAN_OPERATOR_OR)
                        .evaluate(nullRow))
                .isNull();
        assertThat(StreamFusionBooleanCondition.not(unknown).evaluate(nullRow)).isNull();
        assertThat(StreamFusionBooleanCondition.not(unknown).test(nullRow)).isFalse();
    }

    @Test
    void readsNullableBooleanColumnsAsThreeValuedConditions() {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        StreamFusionCondition condition = new StreamFusionBooleanColumnCondition(0, reference);

        assertThat(condition.evaluate(GenericRowData.of(true))).isTrue();
        assertThat(condition.evaluate(GenericRowData.of(false))).isFalse();
        assertThat(condition.evaluate(GenericRowData.of((Object) null))).isNull();
        assertThat(condition.test(GenericRowData.of((Object) null))).isFalse();
    }
}
