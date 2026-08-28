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

import org.apache.flink.table.data.RowData;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.TruthTest;
import tech.streamfusion.proto.plan.v1.TruthTestOperator;

/** Null-safe SQL boolean truth test. */
final class StreamFusionTruthTestCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;

    private final StreamFusionCondition operand;
    private final TruthTestOperator operator;

    private StreamFusionTruthTestCondition(StreamFusionCondition operand, TruthTestOperator operator) {
        this.operand = operand;
        this.operator = operator;
    }

    static StreamFusionTruthTestCondition create(String kind, StreamFusionCondition operand) {
        switch (kind) {
            case "IS_TRUE":
                return new StreamFusionTruthTestCondition(operand, TruthTestOperator.TRUTH_TEST_OPERATOR_IS_TRUE);
            case "IS_FALSE":
                return new StreamFusionTruthTestCondition(operand, TruthTestOperator.TRUTH_TEST_OPERATOR_IS_FALSE);
            case "IS_NOT_TRUE":
                return new StreamFusionTruthTestCondition(operand, TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_TRUE);
            case "IS_NOT_FALSE":
                return new StreamFusionTruthTestCondition(operand, TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_FALSE);
            default:
                throw new IllegalArgumentException("Unsupported truth test " + kind);
        }
    }

    @Override
    public Boolean evaluate(RowData row) {
        Boolean value = operand.evaluate(row);
        switch (operator) {
            case TRUTH_TEST_OPERATOR_IS_TRUE:
                return Boolean.TRUE.equals(value);
            case TRUTH_TEST_OPERATOR_IS_FALSE:
                return Boolean.FALSE.equals(value);
            case TRUTH_TEST_OPERATOR_IS_NOT_TRUE:
                return !Boolean.TRUE.equals(value);
            case TRUTH_TEST_OPERATOR_IS_NOT_FALSE:
                return !Boolean.FALSE.equals(value);
            default:
                throw new IllegalStateException("Unsupported truth test " + operator);
        }
    }

    @Override
    public Expression expression() {
        return Expression.newBuilder()
                .setTruthTest(
                        TruthTest.newBuilder().setOperand(operand.expression()).setOperator(operator))
                .build();
    }
}
