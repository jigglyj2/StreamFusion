/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.unnest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Reflection entry point for parity-safe inner UNNEST over a directly referenced array column. */
public final class StreamFusionArrayUnnestTranslator {
    private StreamFusionArrayUnnestTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input, RowType inputType, RowType outputType, Object invocation) {
        String rejection = unsupportedReason(inputType, outputType, "INNER", invocation, null);
        if (rejection != null) {
            return null;
        }
        int arrayIndex = arrayIndex(invocation);
        StreamFusionArrayUnnestOperator operator =
                new StreamFusionArrayUnnestOperator(inputType, outputType, arrayIndex, withOrdinality(invocation));
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-array-unnest",
                operator,
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    /** Returns null only for the narrow correlate shape whose Flink semantics are reproduced exactly. */
    public static String unsupportedReason(
            RowType inputType, RowType outputType, Object joinType, Object invocation, Object condition) {
        String joinName = joinType instanceof Enum<?> ? ((Enum<?>) joinType).name() : String.valueOf(joinType);
        if (!"INNER".equals(joinName)) {
            return "UNNEST join type " + joinType + " is not accelerated; only inner/cross array UNNEST is supported";
        }
        if (condition != null) {
            return "UNNEST correlate conditions are not accelerated";
        }
        String functionName = String.valueOf(invoke(invoke(invocation, "getOperator"), "getName"));
        boolean withOrdinality = "$UNNEST_ROWS_WITH_ORDINALITY$1".equals(functionName);
        if (!"$UNNEST_ROWS$1".equals(functionName) && !withOrdinality) {
            return "table function " + functionName + " is not StreamFusion array UNNEST";
        }
        List<?> operands = (List<?>) invoke(invocation, "getOperands");
        if (operands.size() != 1 || !operands.get(0).getClass().getSimpleName().equals("RexFieldAccess")) {
            return "array UNNEST requires one directly referenced input field";
        }
        int index = arrayIndex(invocation);
        if (index < 0 || index >= inputType.getFieldCount()) {
            return "array UNNEST field index " + index + " is outside the input row";
        }
        LogicalType collection = inputType.getTypeAt(index);
        if (collection.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            return "UNNEST input " + inputType.getFieldNames().get(index) + " is not an ARRAY";
        }
        LogicalType element = ((ArrayType) collection).getElementType();
        if (!isScalarBoundaryType(element.getTypeRoot())) {
            return "array UNNEST element type " + element + " is not yet supported";
        }
        int appendedFields = withOrdinality ? 2 : 1;
        if (outputType.getFieldCount() != inputType.getFieldCount() + appendedFields) {
            return "array UNNEST output must append its element"
                    + (withOrdinality ? " and ordinality" : "")
                    + " fields";
        }
        for (int field = 0; field < inputType.getFieldCount(); field++) {
            if (!inputType.getTypeAt(field).equals(outputType.getTypeAt(field))) {
                return "array UNNEST output does not preserve input field " + field + " exactly";
            }
        }
        if (!element.equals(outputType.getTypeAt(inputType.getFieldCount()))) {
            return "array UNNEST output element type does not match its ARRAY element type";
        }
        if (withOrdinality) {
            LogicalType ordinality = outputType.getTypeAt(inputType.getFieldCount() + 1);
            if (ordinality.getTypeRoot() != LogicalTypeRoot.INTEGER || ordinality.isNullable()) {
                return "array UNNEST ordinality must be a non-null INT";
            }
        }
        return null;
    }

    public static int arrayIndex(Object invocation) {
        List<?> operands = (List<?>) invoke(invocation, "getOperands");
        Object field = invoke(operands.get(0), "getField");
        return ((Number) invoke(field, "getIndex")).intValue();
    }

    public static boolean withOrdinality(Object invocation) {
        String functionName = String.valueOf(invoke(invoke(invocation, "getOperator"), "getName"));
        return "$UNNEST_ROWS_WITH_ORDINALITY$1".equals(functionName);
    }

    private static boolean isScalarBoundaryType(LogicalTypeRoot type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    private static Object invoke(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot inspect Flink UNNEST " + methodName, e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Flink UNNEST inspection failed", e.getCause());
        }
    }
}
