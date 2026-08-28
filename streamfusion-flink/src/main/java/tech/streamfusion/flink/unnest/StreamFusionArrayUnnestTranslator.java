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
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.UnnestCollection;

/** Reflection entry point for parity-safe UNNEST over a directly referenced array column. */
public final class StreamFusionArrayUnnestTranslator {
    private StreamFusionArrayUnnestTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input, RowType inputType, RowType outputType, Object joinType, Object invocation) {
        String rejection = unsupportedReason(inputType, outputType, joinType, invocation, null);
        if (rejection != null) {
            return null;
        }
        int arrayIndex = arrayIndex(invocation);
        StreamFusionArrayUnnestOperator operator = new StreamFusionArrayUnnestOperator(
                inputType,
                outputType,
                arrayIndex,
                withOrdinality(invocation),
                isLeft(joinType),
                collection(inputType, invocation));
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
        if (!"INNER".equals(joinName) && !"LEFT".equals(joinName)) {
            return "UNNEST join type " + joinType
                    + " is not accelerated; only inner/cross and left ARRAY/MAP UNNEST are supported";
        }
        if (condition != null) {
            return "UNNEST correlate conditions are not accelerated";
        }
        String functionName = String.valueOf(invoke(invoke(invocation, "getOperator"), "getName"));
        boolean withOrdinality = "$UNNEST_ROWS_WITH_ORDINALITY$1".equals(functionName);
        if (!"$UNNEST_ROWS$1".equals(functionName) && !withOrdinality) {
            return "table function " + functionName + " is not StreamFusion collection UNNEST";
        }
        List<?> operands = (List<?>) invoke(invocation, "getOperands");
        if (operands.size() != 1 || !operands.get(0).getClass().getSimpleName().equals("RexFieldAccess")) {
            return "collection UNNEST requires one directly referenced input field";
        }
        int index = arrayIndex(invocation);
        if (index < 0 || index >= inputType.getFieldCount()) {
            return "collection UNNEST field index " + index + " is outside the input row";
        }
        LogicalType collection = inputType.getTypeAt(index);
        if (collection.getTypeRoot() == LogicalTypeRoot.MAP) {
            return unsupportedMapReason(inputType, outputType, joinName, withOrdinality, (MapType) collection);
        }
        if (collection.getTypeRoot() == LogicalTypeRoot.MULTISET) {
            return unsupportedMultisetReason(
                    inputType, outputType, joinName, withOrdinality, (MultisetType) collection);
        }
        if (collection.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            return "UNNEST input " + inputType.getFieldNames().get(index) + " is not ARRAY, MAP, or MULTISET";
        }
        LogicalType element = ((ArrayType) collection).getElementType();
        if (!isSupportedArrayElement(element)) {
            return "array UNNEST element type " + element + " is not yet supported";
        }
        if (withOrdinality && element instanceof RowType && element.isNullable()) {
            return "array UNNEST of nullable ROW WITH ORDINALITY is not accelerated because Flink 2.3 fails its output arity contract for null elements";
        }
        int elementFields = element instanceof RowType ? ((RowType) element).getFieldCount() : 1;
        int appendedFields = elementFields + (withOrdinality ? 1 : 0);
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
        if (element instanceof RowType) {
            RowType row = (RowType) element;
            for (int field = 0; field < row.getFieldCount(); field++) {
                LogicalType expected = row.getTypeAt(field)
                        .copy("LEFT".equals(joinName)
                                || element.isNullable()
                                || row.getTypeAt(field).isNullable());
                if (!expected.equals(outputType.getTypeAt(inputType.getFieldCount() + field))) {
                    return "array UNNEST output field " + field + " does not match its ROW element field";
                }
            }
        } else {
            LogicalType expectedElement = "LEFT".equals(joinName) ? element.copy(true) : element;
            if (!expectedElement.equals(outputType.getTypeAt(inputType.getFieldCount()))) {
                return "array UNNEST output element type does not match its ARRAY element type";
            }
        }
        if (withOrdinality) {
            LogicalType ordinality = outputType.getTypeAt(inputType.getFieldCount() + elementFields);
            boolean expectedNullable = "LEFT".equals(joinName);
            if (ordinality.getTypeRoot() != LogicalTypeRoot.INTEGER || ordinality.isNullable() != expectedNullable) {
                return "array UNNEST ordinality must be " + (expectedNullable ? "a nullable" : "a non-null") + " INT";
            }
        }
        return null;
    }

    private static String unsupportedMapReason(
            RowType inputType, RowType outputType, String joinName, boolean withOrdinality, MapType map) {
        LogicalType key = map.getKeyType();
        LogicalType value = map.getValueType();
        if (!isSupportedElement(key) || !isSupportedArrayElement(value)) {
            return "map UNNEST key/value types " + key + "/" + value + " are not yet supported";
        }
        int appendedFields = 2 + (withOrdinality ? 1 : 0);
        if (outputType.getFieldCount() != inputType.getFieldCount() + appendedFields) {
            return "map UNNEST output must append key, value" + (withOrdinality ? ", and ordinality" : "");
        }
        for (int field = 0; field < inputType.getFieldCount(); field++) {
            if (!inputType.getTypeAt(field).equals(outputType.getTypeAt(field))) {
                return "map UNNEST output does not preserve input field " + field + " exactly";
            }
        }
        boolean left = "LEFT".equals(joinName);
        LogicalType expectedKey = left ? key.copy(true) : key;
        if (!expectedKey.equals(outputType.getTypeAt(inputType.getFieldCount()))) {
            return "map UNNEST output key type does not match its MAP key type";
        }
        LogicalType expectedValue = left ? value.copy(true) : value;
        if (!expectedValue.equals(outputType.getTypeAt(inputType.getFieldCount() + 1))) {
            return "map UNNEST output value type does not match its MAP value type";
        }
        if (withOrdinality) {
            LogicalType ordinality = outputType.getTypeAt(inputType.getFieldCount() + 2);
            if (ordinality.getTypeRoot() != LogicalTypeRoot.INTEGER || ordinality.isNullable() != left) {
                return "map UNNEST ordinality must be " + (left ? "a nullable" : "a non-null") + " INT";
            }
        }
        return null;
    }

    private static String unsupportedMultisetReason(
            RowType inputType, RowType outputType, String joinName, boolean withOrdinality, MultisetType multiset) {
        LogicalType element = multiset.getElementType();
        if (element.isNullable()) {
            return "multiset UNNEST nullable elements are not yet supported by the Arrow map boundary";
        }
        if (!isSupportedElement(element)) {
            return "multiset UNNEST element type " + element + " is not yet supported";
        }
        int elementFields = element instanceof RowType ? ((RowType) element).getFieldCount() : 1;
        int appendedFields = elementFields + (withOrdinality ? 1 : 0);
        if (outputType.getFieldCount() != inputType.getFieldCount() + appendedFields) {
            return "multiset UNNEST output must append its element" + (withOrdinality ? " and ordinality" : "");
        }
        for (int field = 0; field < inputType.getFieldCount(); field++) {
            if (!inputType.getTypeAt(field).equals(outputType.getTypeAt(field))) {
                return "multiset UNNEST output does not preserve input field " + field + " exactly";
            }
        }
        boolean left = "LEFT".equals(joinName);
        if (element instanceof RowType) {
            RowType row = (RowType) element;
            for (int field = 0; field < row.getFieldCount(); field++) {
                LogicalType expected =
                        row.getTypeAt(field).copy(left || row.getTypeAt(field).isNullable());
                if (!expected.equals(outputType.getTypeAt(inputType.getFieldCount() + field))) {
                    return "multiset UNNEST output field " + field + " does not match its ROW element field";
                }
            }
        } else {
            LogicalType expectedElement = left ? element.copy(true) : element;
            if (!expectedElement.equals(outputType.getTypeAt(inputType.getFieldCount()))) {
                return "multiset UNNEST output element type does not match its MULTISET element type";
            }
        }
        if (withOrdinality) {
            LogicalType ordinality = outputType.getTypeAt(inputType.getFieldCount() + elementFields);
            if (ordinality.getTypeRoot() != LogicalTypeRoot.INTEGER || ordinality.isNullable() != left) {
                return "multiset UNNEST ordinality must be " + (left ? "a nullable" : "a non-null") + " INT";
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

    public static boolean isLeft(Object joinType) {
        return "LEFT".equals(joinType instanceof Enum<?> ? ((Enum<?>) joinType).name() : String.valueOf(joinType));
    }

    public static UnnestCollection collection(RowType inputType, Object invocation) {
        LogicalTypeRoot root = inputType.getTypeAt(arrayIndex(invocation)).getTypeRoot();
        if (root == LogicalTypeRoot.MAP) {
            return UnnestCollection.UNNEST_COLLECTION_MAP;
        }
        if (root == LogicalTypeRoot.MULTISET) {
            return UnnestCollection.UNNEST_COLLECTION_MULTISET;
        }
        return UnnestCollection.UNNEST_COLLECTION_ARRAY;
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

    private static boolean isSupportedElement(LogicalType element) {
        if (isScalarBoundaryType(element.getTypeRoot())) {
            return true;
        }
        if (!(element instanceof RowType)) {
            return false;
        }
        RowType row = (RowType) element;
        return row.getFieldCount() > 0
                && row.getChildren().stream().allMatch(child -> isScalarBoundaryType(child.getTypeRoot()));
    }

    private static boolean isSupportedArrayElement(LogicalType element) {
        if (isSupportedElement(element)) {
            return true;
        }
        if (element instanceof RowType) {
            RowType row = (RowType) element;
            return row.getFieldCount() > 0
                    && row.getChildren().stream().allMatch(child -> {
                        if (isScalarBoundaryType(child.getTypeRoot())) {
                            return true;
                        }
                        return child instanceof ArrayType
                                && isScalarBoundaryType(
                                        ((ArrayType) child).getElementType().getTypeRoot());
                    });
        }
        return element instanceof ArrayType
                && isScalarBoundaryType(((ArrayType) element).getElementType().getTypeRoot());
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
