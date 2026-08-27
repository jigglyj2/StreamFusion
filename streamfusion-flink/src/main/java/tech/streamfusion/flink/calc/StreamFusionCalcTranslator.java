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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
public final class StreamFusionCalcTranslator {
    private StreamFusionCalcTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            List<?> projections,
            Object condition) {
        if (!isSupportedCalc(inputType, outputType, projections, condition)) {
            return null;
        }

        List<Integer> inputIndexes =
                projections.stream().map(StreamFusionCalcTranslator::inputIndex).collect(Collectors.toList());
        StreamFusionIntComparison comparison = comparison(condition);
        StreamFusionIdentityCalcOperator operator =
                new StreamFusionIdentityCalcOperator(inputType, outputType, inputIndexes, comparison);
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-identity-calc",
                operator,
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    public static boolean canTranslate(RowType inputType, RowType outputType, List<?> projections, Object condition) {
        return isSupportedCalc(inputType, outputType, projections, condition);
    }

    private static boolean isSupportedCalc(
            RowType inputType, RowType outputType, List<?> projections, Object condition) {
        if (projections.isEmpty()
                || outputType.getFieldCount() != projections.size()
                || projections.stream().anyMatch(expression -> inputIndex(expression) < 0)) {
            return false;
        }
        for (int outputIndex = 0; outputIndex < projections.size(); outputIndex++) {
            int inputIndex = inputIndex(projections.get(outputIndex));
            if (inputIndex >= inputType.getFieldCount()
                    || !isSupportedProjectionType(
                            inputType.getTypeAt(inputIndex).getTypeRoot())
                    || !inputType.getTypeAt(inputIndex).equals(outputType.getTypeAt(outputIndex))) {
                return false;
            }
        }
        if (condition == null) {
            return true;
        }
        StreamFusionIntComparison comparison = comparison(condition);
        return comparison != null
                && inputType.getTypeAt(comparison.inputIndex()).getTypeRoot() == LogicalTypeRoot.INTEGER;
    }

    private static boolean isSupportedProjectionType(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.FLOAT
                || type == LogicalTypeRoot.DOUBLE
                || type == LogicalTypeRoot.BOOLEAN
                || type == LogicalTypeRoot.CHAR
                || type == LogicalTypeRoot.VARCHAR
                || type == LogicalTypeRoot.BINARY
                || type == LogicalTypeRoot.VARBINARY
                || type == LogicalTypeRoot.DECIMAL
                || type == LogicalTypeRoot.DATE
                || type == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || type == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static StreamFusionIntComparison comparison(Object condition) {
        if (condition == null) {
            return null;
        }
        ComparisonOperator operator =
                comparisonOperator(invoke(condition, "getKind").toString());
        if (operator == null) {
            return null;
        }
        List<?> operands = (List<?>) invoke(condition, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        int leftInput = inputIndex(operands.get(0));
        int rightInput = inputIndex(operands.get(1));
        Integer leftLiteral = integerLiteral(operands.get(0));
        Integer rightLiteral = integerLiteral(operands.get(1));
        if (leftInput >= 0 && rightLiteral != null) {
            return new StreamFusionIntComparison(leftInput, rightLiteral, operator, true);
        }
        if (rightInput >= 0 && leftLiteral != null) {
            return new StreamFusionIntComparison(rightInput, leftLiteral, operator, false);
        }
        return null;
    }

    private static Integer integerLiteral(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexLiteral")) {
            return null;
        }
        Object value = invoke(expression, "getValueAs", Class.class, Integer.class);
        return value instanceof Integer ? (Integer) value : null;
    }

    private static ComparisonOperator comparisonOperator(String kind) {
        switch (kind) {
            case "EQUALS":
                return ComparisonOperator.COMPARISON_OPERATOR_EQUAL;
            case "NOT_EQUALS":
                return ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL;
            case "LESS_THAN":
                return ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN;
            case "LESS_THAN_OR_EQUAL":
                return ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL;
            case "GREATER_THAN":
                return ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN;
            case "GREATER_THAN_OR_EQUAL":
                return ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL;
            default:
                return null;
        }
    }

    private static int inputIndex(Object expression) {
        if (!expression.getClass().getSimpleName().equals("RexInputRef")) {
            return -1;
        }
        Object index = invoke(expression, "getIndex");
        return index instanceof Integer ? (Integer) index : -1;
    }

    private static Object invoke(Object target, String methodName) {
        return invoke(target, methodName, null, null);
    }

    private static Object invoke(Object target, String methodName, Class<?> parameterType, Object argument) {
        try {
            Method method = parameterType == null
                    ? target.getClass().getMethod(methodName)
                    : target.getClass().getMethod(methodName, parameterType);
            return parameterType == null ? method.invoke(target) : method.invoke(target, argument);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalStateException(
                    "Could not inspect planner expression " + target.getClass().getName(), exception);
        }
    }
}
