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
import tech.streamfusion.proto.plan.v1.ArbitraryLogarithm;
import tech.streamfusion.proto.plan.v1.ArcCosine;
import tech.streamfusion.proto.plan.v1.ArcSine;
import tech.streamfusion.proto.plan.v1.ArcTangent;
import tech.streamfusion.proto.plan.v1.ArcTangent2;
import tech.streamfusion.proto.plan.v1.Ceiling;
import tech.streamfusion.proto.plan.v1.CommonLogarithm;
import tech.streamfusion.proto.plan.v1.Cosine;
import tech.streamfusion.proto.plan.v1.Cotangent;
import tech.streamfusion.proto.plan.v1.Degrees;
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.Exponential;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Floor;
import tech.streamfusion.proto.plan.v1.HyperbolicSine;
import tech.streamfusion.proto.plan.v1.HyperbolicTangent;
import tech.streamfusion.proto.plan.v1.NaturalLogarithm;
import tech.streamfusion.proto.plan.v1.Power;
import tech.streamfusion.proto.plan.v1.Radians;
import tech.streamfusion.proto.plan.v1.Sign;
import tech.streamfusion.proto.plan.v1.Sine;
import tech.streamfusion.proto.plan.v1.SquareRoot;
import tech.streamfusion.proto.plan.v1.Tangent;

/** Translates numeric scalar functions into the native expression protocol. */
final class StreamFusionNumericFunctionTranslator extends StreamFusionRexSupport {
    private StreamFusionNumericFunctionTranslator() {}

    static String failureReason(Object expression) {
        if ("COSH".equals(functionName(expression))) {
            return "COSH stays on Flink because DataFusion differs from Flink by one ULP for finite DOUBLE inputs";
        }
        if ("LOG2".equals(functionName(expression))) {
            return "LOG2 stays on Flink because DataFusion differs from Flink by one ULP for finite DOUBLE inputs";
        }
        if ("POWER".equals(functionName(expression)) && hasNoArgMethod(expression, "getOperands")) {
            List<?> operands = (List<?>) invoke(expression, "getOperands");
            if (operands.size() == 2) {
                Double exponent = literal(operands.get(1), Double.class);
                if (exponent == null || exponent < 0.0d) {
                    return "POWER stays on Flink unless its exponent is a nonnegative DOUBLE literal because DataFusion errors on zero raised to a negative power";
                }
            }
        }
        if ("ROUND".equals(functionName(expression))) {
            return "ROUND stays on Flink because floating ROUND has data-dependent error semantics for non-finite values that DataFusion does not preserve";
        }
        return null;
    }

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
            if (exponent == null || exponent < 0.0d) {
                return null;
            }
            if (exponent != null && Double.compare(exponent, 0.5d) == 0) {
                return unary(expression, inputType, expectedType, UnaryKind.SQUARE_ROOT);
            }
            Expression baseExpression =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, expectedType);
            Expression exponentExpression =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, expectedType);
            if (baseExpression != null && exponentExpression != null) {
                return Expression.newBuilder()
                        .setPower(Power.newBuilder().setBase(baseExpression).setExponent(exponentExpression))
                        .build();
            }
        }
        if ("ATAN2".equals(function) && operands.size() == 2 && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            Expression y =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, expectedType);
            Expression x =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, expectedType);
            if (y != null && x != null) {
                return Expression.newBuilder()
                        .setArcTangent2(ArcTangent2.newBuilder().setY(y).setX(x))
                        .build();
            }
        }
        if ("LOG".equals(function) && operands.size() == 2 && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            Expression base =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, expectedType);
            Expression value =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, expectedType);
            if (base != null && value != null) {
                return Expression.newBuilder()
                        .setArbitraryLogarithm(
                                ArbitraryLogarithm.newBuilder().setBase(base).setValue(value))
                        .build();
            }
        }
        if ("EXP".equals(function) && operands.size() == 1 && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            return unary(expression, inputType, expectedType, UnaryKind.EXPONENTIAL);
        }
        if (operands.size() == 1 && expectedType.getTypeRoot() == LogicalTypeRoot.DOUBLE) {
            if ("SIN".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.SINE);
            }
            if ("COS".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.COSINE);
            }
            if ("TAN".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.TANGENT);
            }
            if ("COT".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.COTANGENT);
            }
            if ("LN".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.NATURAL_LOGARITHM);
            }
            if ("LOG".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.NATURAL_LOGARITHM);
            }
            if ("LOG10".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.COMMON_LOGARITHM);
            }
            if ("SINH".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.HYPERBOLIC_SINE);
            }
            if ("TANH".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.HYPERBOLIC_TANGENT);
            }
            if ("ASIN".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.ARC_SINE);
            }
            if ("ACOS".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.ARC_COSINE);
            }
            if ("ATAN".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.ARC_TANGENT);
            }
            if ("DEGREES".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.DEGREES);
            }
            if ("RADIANS".equals(function)) {
                return unary(expression, inputType, expectedType, UnaryKind.RADIANS);
            }
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
            case SINE:
                return result.setSine(Sine.newBuilder().setOperand(operand)).build();
            case COSINE:
                return result.setCosine(Cosine.newBuilder().setOperand(operand)).build();
            case TANGENT:
                return result.setTangent(Tangent.newBuilder().setOperand(operand))
                        .build();
            case COTANGENT:
                return result.setCotangent(Cotangent.newBuilder().setOperand(operand))
                        .build();
            case NATURAL_LOGARITHM:
                return result.setNaturalLogarithm(NaturalLogarithm.newBuilder().setOperand(operand))
                        .build();
            case COMMON_LOGARITHM:
                return result.setCommonLogarithm(CommonLogarithm.newBuilder().setOperand(operand))
                        .build();
            case HYPERBOLIC_SINE:
                return result.setHyperbolicSine(HyperbolicSine.newBuilder().setOperand(operand))
                        .build();
            case HYPERBOLIC_TANGENT:
                return result.setHyperbolicTangent(
                                HyperbolicTangent.newBuilder().setOperand(operand))
                        .build();
            case ARC_SINE:
                return result.setArcSine(ArcSine.newBuilder().setOperand(operand))
                        .build();
            case ARC_COSINE:
                return result.setArcCosine(ArcCosine.newBuilder().setOperand(operand))
                        .build();
            case ARC_TANGENT:
                return result.setArcTangent(ArcTangent.newBuilder().setOperand(operand))
                        .build();
            case DEGREES:
                return result.setDegrees(Degrees.newBuilder().setOperand(operand))
                        .build();
            case RADIANS:
                return result.setRadians(Radians.newBuilder().setOperand(operand))
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
        EXPONENTIAL,
        SINE,
        COSINE,
        TANGENT,
        COTANGENT,
        NATURAL_LOGARITHM,
        COMMON_LOGARITHM,
        HYPERBOLIC_SINE,
        HYPERBOLIC_TANGENT,
        ARC_SINE,
        ARC_COSINE,
        ARC_TANGENT,
        DEGREES,
        RADIANS
    }
}
