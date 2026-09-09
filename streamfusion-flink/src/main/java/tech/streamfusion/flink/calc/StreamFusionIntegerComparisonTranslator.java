/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Cast;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Expression;

/** Widen completed signed-integer operands without changing their internal arithmetic width. */
final class StreamFusionIntegerComparisonTranslator {
    private StreamFusionIntegerComparisonTranslator() {}

    static Expression translate(
            Object left,
            Object right,
            LogicalType leftType,
            LogicalType rightType,
            ComparisonOperator operator,
            RowType inputType) {
        if (leftType == null
                || rightType == null
                || leftType.getTypeRoot() == rightType.getTypeRoot()
                || !signed(leftType.getTypeRoot())
                || !signed(rightType.getTypeRoot())) return null;
        Expression lhs = widen(left, leftType, inputType);
        Expression rhs = widen(right, rightType, inputType);
        if (lhs == null || rhs == null) return null;
        return Expression.newBuilder()
                .setComparison(
                        Comparison.newBuilder().setLeft(lhs).setRight(rhs).setOperator(operator))
                .build();
    }

    private static Expression widen(Object expression, LogicalType type, RowType inputType) {
        Expression operand = StreamFusionProjectionTranslator.projectionExpression(expression, inputType, type);
        if (operand == null || type.getTypeRoot() == LogicalTypeRoot.BIGINT) return operand;
        return Expression.newBuilder()
                .setCast(Cast.newBuilder()
                        .setOperand(operand)
                        .setTargetType(StreamFusionCastSupport.targetType(LogicalTypeRoot.BIGINT, type.isNullable()))
                        .setKind(StreamFusionCastSupport.kind(type.getTypeRoot(), LogicalTypeRoot.BIGINT)))
                .build();
    }

    private static boolean signed(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT;
    }
}
