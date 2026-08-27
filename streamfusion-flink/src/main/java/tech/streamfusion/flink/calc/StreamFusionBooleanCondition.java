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
import tech.streamfusion.proto.plan.v1.BooleanBinary;
import tech.streamfusion.proto.plan.v1.BooleanNot;
import tech.streamfusion.proto.plan.v1.BooleanOperator;
import tech.streamfusion.proto.plan.v1.Expression;

/** SQL three-valued AND, OR, or NOT condition. */
final class StreamFusionBooleanCondition implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;
    private final StreamFusionCondition left;
    private final StreamFusionCondition right;
    private final BooleanOperator operator;
    private final boolean negated;

    private StreamFusionBooleanCondition(
            StreamFusionCondition left, StreamFusionCondition right, BooleanOperator operator, boolean negated) {
        this.left = left;
        this.right = right;
        this.operator = operator;
        this.negated = negated;
    }

    static StreamFusionBooleanCondition binary(
            StreamFusionCondition left, StreamFusionCondition right, BooleanOperator operator) {
        return new StreamFusionBooleanCondition(left, right, operator, false);
    }

    static StreamFusionBooleanCondition not(StreamFusionCondition operand) {
        return new StreamFusionBooleanCondition(operand, null, null, true);
    }

    @Override
    public Boolean evaluate(RowData row) {
        Boolean leftValue = left.evaluate(row);
        if (negated) {
            return leftValue == null ? null : !leftValue;
        }
        Boolean rightValue = right.evaluate(row);
        if (operator == BooleanOperator.BOOLEAN_OPERATOR_AND) {
            if (Boolean.FALSE.equals(leftValue) || Boolean.FALSE.equals(rightValue)) {
                return false;
            }
            return leftValue == null || rightValue == null ? null : true;
        }
        if (Boolean.TRUE.equals(leftValue) || Boolean.TRUE.equals(rightValue)) {
            return true;
        }
        return leftValue == null || rightValue == null ? null : false;
    }

    @Override
    public Expression expression() {
        if (negated) {
            return Expression.newBuilder()
                    .setBooleanNot(BooleanNot.newBuilder().setOperand(left.expression()))
                    .build();
        }
        return Expression.newBuilder()
                .setBooleanBinary(BooleanBinary.newBuilder()
                        .setLeft(left.expression())
                        .setRight(right.expression())
                        .setOperator(operator))
                .build();
    }
}
