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
        Integer conditionInputIndex =
                condition == null ? null : inputIndex(((List<?>) invoke(condition, "getOperands")).get(0));
        Integer minimum = minimumValue(condition, conditionInputIndex == null ? -1 : conditionInputIndex);
        StreamFusionIdentityCalcOperator operator =
                new StreamFusionIdentityCalcOperator(inputType, outputType, inputIndexes, conditionInputIndex, minimum);
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
        List<?> operands = (List<?>) invoke(condition, "getOperands");
        if (operands.size() != 2) {
            return false;
        }
        int conditionInputIndex = inputIndex(operands.get(0));
        return conditionInputIndex >= 0
                && inputType.getTypeAt(conditionInputIndex).getTypeRoot() == LogicalTypeRoot.INTEGER
                && minimumValue(condition, conditionInputIndex) != null;
    }

    private static boolean isSupportedProjectionType(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.BOOLEAN
                || type == LogicalTypeRoot.CHAR
                || type == LogicalTypeRoot.VARCHAR;
    }

    private static Integer minimumValue(Object condition, int inputIndex) {
        if (condition == null) {
            return null;
        }
        if (!"GREATER_THAN_OR_EQUAL".equals(invoke(condition, "getKind").toString())) {
            return null;
        }
        List<?> operands = (List<?>) invoke(condition, "getOperands");
        if (operands.size() != 2 || inputIndex(operands.get(0)) != inputIndex) {
            return null;
        }
        Object value = invoke(operands.get(1), "getValueAs", Class.class, Integer.class);
        return value instanceof Integer ? (Integer) value : null;
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
