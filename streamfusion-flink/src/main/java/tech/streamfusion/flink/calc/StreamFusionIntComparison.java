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

import java.io.Serializable;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

/** Supported integer comparison, including the original SQL operand order. */
final class StreamFusionIntComparison implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int inputIndex;
    private final int literal;
    private final ComparisonOperator operator;
    private final boolean inputOnLeft;

    StreamFusionIntComparison(int inputIndex, int literal, ComparisonOperator operator, boolean inputOnLeft) {
        this.inputIndex = inputIndex;
        this.literal = literal;
        this.operator = operator;
        this.inputOnLeft = inputOnLeft;
    }

    int inputIndex() {
        return inputIndex;
    }

    int literal() {
        return literal;
    }

    ComparisonOperator operator() {
        return operator;
    }

    boolean inputOnLeft() {
        return inputOnLeft;
    }

    boolean test(RowData row) {
        if (row.isNullAt(inputIndex)) {
            return false;
        }
        int left = inputOnLeft ? row.getInt(inputIndex) : literal;
        int right = inputOnLeft ? literal : row.getInt(inputIndex);
        switch (operator) {
            case COMPARISON_OPERATOR_EQUAL:
                return left == right;
            case COMPARISON_OPERATOR_NOT_EQUAL:
                return left != right;
            case COMPARISON_OPERATOR_LESS_THAN:
                return left < right;
            case COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
                return left <= right;
            case COMPARISON_OPERATOR_GREATER_THAN:
                return left > right;
            case COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
                return left >= right;
            default:
                throw new IllegalStateException("Unsupported comparison operator " + operator);
        }
    }
}
