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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;

/** A same-type ordered comparison between two input columns. */
final class StreamFusionColumnComparison implements StreamFusionCondition {
    private static final long serialVersionUID = 1L;

    private final int leftIndex;
    private final int rightIndex;
    private final LogicalType type;
    private final ComparisonOperator operator;

    StreamFusionColumnComparison(int leftIndex, int rightIndex, LogicalType type, ComparisonOperator operator) {
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
        this.type = type;
        this.operator = operator;
    }

    static boolean supports(LogicalTypeRoot root) {
        return root == LogicalTypeRoot.BOOLEAN
                || root == LogicalTypeRoot.TINYINT
                || root == LogicalTypeRoot.SMALLINT
                || root == LogicalTypeRoot.INTEGER
                || root == LogicalTypeRoot.BIGINT
                || root == LogicalTypeRoot.VARCHAR
                || root == LogicalTypeRoot.DATE
                || root == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.DECIMAL;
    }

    @Override
    public Boolean evaluate(RowData row) {
        boolean leftNull = row.isNullAt(leftIndex);
        boolean rightNull = row.isNullAt(rightIndex);
        if (operator == ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM) {
            return leftNull != rightNull || (!leftNull && compare(row) != 0);
        }
        if (operator == ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM) {
            return leftNull == rightNull && (leftNull || compare(row) == 0);
        }
        if (leftNull || rightNull) {
            return null;
        }
        int comparison = compare(row);
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
            default:
                throw new IllegalStateException("Unsupported comparison operator " + operator);
        }
    }

    private int compare(RowData row) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Boolean.compare(row.getBoolean(leftIndex), row.getBoolean(rightIndex));
            case TINYINT:
                return Byte.compare(row.getByte(leftIndex), row.getByte(rightIndex));
            case SMALLINT:
                return Short.compare(row.getShort(leftIndex), row.getShort(rightIndex));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.compare(row.getInt(leftIndex), row.getInt(rightIndex));
            case BIGINT:
                return Long.compare(row.getLong(leftIndex), row.getLong(rightIndex));
            case VARCHAR:
                return row.getString(leftIndex).compareTo(row.getString(rightIndex));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int timestampPrecision = ((TimestampType) type).getPrecision();
                return row.getTimestamp(leftIndex, timestampPrecision)
                        .compareTo(row.getTimestamp(rightIndex, timestampPrecision));
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                return row.getDecimal(leftIndex, decimal.getPrecision(), decimal.getScale())
                        .compareTo(row.getDecimal(rightIndex, decimal.getPrecision(), decimal.getScale()));
            default:
                throw new IllegalStateException("Unsupported comparison type " + type);
        }
    }

    @Override
    public Expression expression() {
        Expression left = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(leftIndex))
                .build();
        Expression right = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(rightIndex))
                .build();
        return Expression.newBuilder()
                .setComparison(
                        Comparison.newBuilder().setLeft(left).setRight(right).setOperator(operator))
                .build();
    }
}
