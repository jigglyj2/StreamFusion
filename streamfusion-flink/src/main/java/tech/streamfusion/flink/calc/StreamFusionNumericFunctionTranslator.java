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

import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.AbsoluteValue;
import tech.streamfusion.proto.plan.v1.Ceiling;
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.Exponential;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Floor;
import tech.streamfusion.proto.plan.v1.Sign;
import tech.streamfusion.proto.plan.v1.SquareRoot;

/** Translates numeric scalar functions into the native expression protocol. */
final class StreamFusionNumericFunctionTranslator extends StreamFusionRexSupport {
    private StreamFusionNumericFunctionTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!hasNoArgMethod(expression, "getOperands")) {
            return null;
        }
        String function = functionName(expression);
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if ("ABS".equals(function) && operands.size() == 1 && isNumeric(expectedType.getTypeRoot())) {
            return unary(expression, inputType, expectedType, UnaryKind.ABSOLUTE);
        }
        if ("CEIL".equals(function) && operands.size() == 1 && isNonDecimalNumeric(expectedType.getTypeRoot())) {
            return unary(expression, inputType, expectedType, UnaryKind.CEILING);
        }
        if ("FLOOR".equals(function) && operands.size() == 1 && isNonDecimalNumeric(expectedType.getTypeRoot())) {
            return unary(expression, inputType, expectedType, UnaryKind.FLOOR);
        }
        if ("SIGN".equals(function) && operands.size() == 1 && isSignNumeric(expectedType.getTypeRoot())) {
            return unary(expression, inputType, expectedType, UnaryKind.SIGN);
        }
        if ("POWER".equals(function) && operands.size() == 2 && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            Double exponent = literal(operands.get(1), Double.class);
            if (exponent != null && Double.compare(exponent, 0.5d) == 0) {
                return unary(expression, inputType, expectedType, UnaryKind.SQUARE_ROOT);
            }
        }
        if ("EXP".equals(function) && operands.size() == 1 && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            return unary(expression, inputType, expectedType, UnaryKind.EXPONENTIAL);
        }
        if (("PI".equals(function) || "E".equals(function))
                && operands.isEmpty()
                && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            double value = "PI".equals(function) ? Math.PI : Math.E;
            return Expression.newBuilder()
                    .setDoubleLiteral(DoubleLiteral.newBuilder().setValue(value))
                    .build();
        }
        return null;
    }

    private static Expression unary(Object expression, RowType inputType, LogicalType expectedType, UnaryKind kind) {
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        Expression operand =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, expectedType);
        if (operand == null) {
            return null;
        }
        Expression.Builder result = Expression.newBuilder();
        switch (kind) {
            case ABSOLUTE:
                return result.setAbsoluteValue(AbsoluteValue.newBuilder().setOperand(operand))
                        .build();
            case CEILING:
                return result.setCeiling(Ceiling.newBuilder().setOperand(operand))
                        .build();
            case FLOOR:
                return result.setFloor(Floor.newBuilder().setOperand(operand)).build();
            case SIGN:
                return result.setSign(Sign.newBuilder().setOperand(operand)).build();
            case SQUARE_ROOT:
                return result.setSquareRoot(SquareRoot.newBuilder().setOperand(operand))
                        .build();
            case EXPONENTIAL:
                return result.setExponential(Exponential.newBuilder().setOperand(operand))
                        .build();
            default:
                throw new IllegalStateException("Unknown numeric unary function " + kind);
        }
    }

    private static boolean isSignNumeric(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE
                || type == LogicalTypeRoot.DECIMAL;
    }

    private static boolean isNonDecimalNumeric(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE;
    }

    private static boolean isNumeric(LogicalTypeRoot type) {
        return isNonDecimalNumeric(type) || type == LogicalTypeRoot.DECIMAL;
    }

    private enum UnaryKind {
        ABSOLUTE,
        CEILING,
        FLOOR,
        SIGN,
        SQUARE_ROOT,
        EXPONENTIAL
    }
}
