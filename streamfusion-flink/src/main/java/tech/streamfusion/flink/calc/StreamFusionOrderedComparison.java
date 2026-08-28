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
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;

/** Shared null, operand-order, operator, and protobuf logic for ordered comparisons. */
abstract class StreamFusionOrderedComparison implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;
    private final int inputIndex;
    private final ComparisonOperator operator;
    private final boolean inputOnLeft;
    private final Expression literal;
    private final Expression inputExpression;

    StreamFusionOrderedComparison(
            int inputIndex, ComparisonOperator operator, boolean inputOnLeft, Expression literal) {
        this(
                inputIndex,
                operator,
                inputOnLeft,
                literal,
                Expression.newBuilder()
                        .setInputReference(InputReference.newBuilder().setIndex(inputIndex))
                        .build());
    }

    StreamFusionOrderedComparison(
            int inputIndex,
            ComparisonOperator operator,
            boolean inputOnLeft,
            Expression literal,
            Expression inputExpression) {
        this.inputIndex = inputIndex;
        this.operator = operator;
        this.inputOnLeft = inputOnLeft;
        this.literal = literal;
        this.inputExpression = inputExpression;
    }

    final int inputIndex() {
        return inputIndex;
    }

    protected abstract int compareInputToLiteral(RowData row);

    @Override
    public final Boolean evaluate(RowData row) {
        if (row.isNullAt(inputIndex)) {
            if (operator == ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM) {
                return true;
            }
            if (operator == ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM) {
                return false;
            }
            return null;
        }
        int comparison = compareInputToLiteral(row);
        if (!inputOnLeft) {
            comparison = -comparison;
        }
        switch (operator) {
            case COMPARISON_OPERATOR_EQUAL:
                return comparison == 0;
            case COMPARISON_OPERATOR_NOT_EQUAL:
                return comparison != 0;
            case COMPARISON_OPERATOR_LESS_THAN:
                return comparison < 0;
            case COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
                return comparison <= 0;
            case COMPARISON_OPERATOR_GREATER_THAN:
                return comparison > 0;
            case COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
                return comparison >= 0;
            case COMPARISON_OPERATOR_IS_DISTINCT_FROM:
                return comparison != 0;
            case COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM:
                return comparison == 0;
            default:
                throw new IllegalStateException("Unsupported comparison operator " + operator);
        }
    }

    @Override
    public final Expression expression() {
        return Expression.newBuilder()
                .setComparison(Comparison.newBuilder()
                        .setLeft(inputOnLeft ? inputExpression : literal)
                        .setRight(inputOnLeft ? literal : inputExpression)
                        .setOperator(operator))
                .build();
    }
}
