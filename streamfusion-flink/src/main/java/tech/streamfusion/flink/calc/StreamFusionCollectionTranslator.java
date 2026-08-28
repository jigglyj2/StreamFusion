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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.ArrayAppend;
import tech.streamfusion.proto.plan.v1.ArrayConcat;
import tech.streamfusion.proto.plan.v1.ArrayContains;
import tech.streamfusion.proto.plan.v1.ArrayJoin;
import tech.streamfusion.proto.plan.v1.ArrayMaximum;
import tech.streamfusion.proto.plan.v1.ArrayMinimum;
import tech.streamfusion.proto.plan.v1.ArrayPosition;
import tech.streamfusion.proto.plan.v1.ArrayPrepend;
import tech.streamfusion.proto.plan.v1.ArrayRemove;
import tech.streamfusion.proto.plan.v1.ArrayReverse;
import tech.streamfusion.proto.plan.v1.ArraySlice;
import tech.streamfusion.proto.plan.v1.ArraySort;
import tech.streamfusion.proto.plan.v1.Cardinality;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Split;

/** Collection functions kept separate from complex access-path translation. */
final class StreamFusionCollectionTranslator extends StreamFusionComplexTypeSupport {
    private StreamFusionCollectionTranslator() {}

    static String failureReason(Object expression, RowType inputType) {
        String function = functionName(expression);
        java.util.List<?> operands = hasNoArgMethod(expression, "getOperands")
                ? (java.util.List<?>) invoke(expression, "getOperands")
                : java.util.Collections.emptyList();
        if ("CARDINALITY".equals(function) && operands.size() == 1) {
            LogicalType collection = logicalType(operands.get(0), inputType);
            if (collection instanceof ArrayType && ((ArrayType) collection).getElementType() instanceof ArrayType) {
                return "nested ARRAY CARDINALITY stays on Flink because DataFusion recursively counts leaf "
                        + "elements while Flink counts the outer array";
            }
        }
        if ("ARRAY_CONTAINS".equals(function) && operands.size() == 2) {
            LogicalType needle = logicalType(operands.get(1), inputType);
            if (needle != null && needle.isNullable()) {
                return "ARRAY_CONTAINS with a nullable needle stays on Flink because Flink searches for null "
                        + "while DataFusion returns null without searching";
            }
        }
        if ("ARRAY_REMOVE".equals(function) && operands.size() == 2) {
            LogicalType needle = logicalType(operands.get(1), inputType);
            if (needle != null && needle.isNullable()) {
                return "ARRAY_REMOVE with a nullable needle stays on Flink because Flink removes null elements while DataFusion returns null";
            }
        }
        if (("ARRAY_MIN".equals(function) || "ARRAY_MAX".equals(function)) && operands.size() == 1) {
            LogicalType collection = logicalType(operands.get(0), inputType);
            if (collection instanceof ArrayType) {
                LogicalTypeRoot element =
                        ((ArrayType) collection).getElementType().getTypeRoot();
                if (element == LogicalTypeRoot.FLOAT || element == LogicalTypeRoot.DOUBLE) {
                    return "floating-point ARRAY_MIN and ARRAY_MAX stay on Flink because Flink and DataFusion order NaN differently";
                }
            }
        }
        if ("ARRAY_JOIN".equals(function) && (operands.size() == 2 || operands.size() == 3)) {
            if (literal(operands.get(1), String.class) == null) {
                return "ARRAY_JOIN stays on Flink unless its delimiter is a non-null literal because DataFusion applies one delimiter to the whole batch";
            }
            if (operands.size() == 3 && literal(operands.get(2), String.class) == null) {
                return "ARRAY_JOIN stays on Flink unless its null replacement is a non-null literal because DataFusion applies one replacement to the whole batch";
            }
        }
        if ("SPLIT".equals(function) && operands.size() == 2) {
            String delimiter = literal(operands.get(1), String.class);
            if (delimiter == null) {
                return "SPLIT stays on Flink unless its delimiter is a non-null literal";
            }
            if (delimiter.isEmpty()) {
                return "SPLIT with an empty delimiter stays on Flink because Flink splits into Unicode characters while DataFusion retains the whole string";
            }
        }
        if ("ARRAY_SORT".equals(function) && !operands.isEmpty()) {
            LogicalType array = logicalType(operands.get(0), inputType);
            if (array instanceof ArrayType
                    && !supportsArrayOrdering(
                            ((ArrayType) array).getElementType().getTypeRoot())) {
                return "ARRAY_SORT stays on Flink for element types whose ordering is not parity-approved; floating-point NaN ordering is intentionally excluded";
            }
            for (int index = 1; index < operands.size(); index++) {
                if (literal(operands.get(index), Boolean.class) == null) {
                    return "ARRAY_SORT stays on Flink unless its ascending and null-order controls are non-null boolean literals";
                }
            }
        }
        if ("ARRAY_SLICE".equals(function) && (operands.size() == 2 || operands.size() == 3)) {
            for (int index = 1; index < operands.size(); index++) {
                if (literal(operands.get(index), Integer.class) == null) {
                    return "ARRAY_SLICE stays on Flink unless its start and optional end positions are non-null integer literals";
                }
            }
        }
        if ("ELEMENT".equals(function) && operands.size() == 1) {
            return "ELEMENT stays on Flink because Flink raises a runtime error for arrays with more than one element while DataFusion indexed access does not";
        }
        return null;
    }

    static Expression cardinality(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"CARDINALITY".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType collectionType = logicalType(operands.get(0), inputType);
        if (!(collectionType instanceof ArrayType) && !(collectionType instanceof MapType)) {
            return null;
        }
        if (collectionType instanceof ArrayType && ((ArrayType) collectionType).getElementType() instanceof ArrayType) {
            return null;
        }
        Expression collection =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, collectionType);
        return collection == null
                ? null
                : Expression.newBuilder()
                        .setCardinality(Cardinality.newBuilder().setCollection(collection))
                        .build();
    }

    static Expression arrayContains(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_CONTAINS".equals(functionName(expression))
                || expectedType.getTypeRoot() != LogicalTypeRoot.BOOLEAN) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        LogicalType collectionType = logicalType(operands.get(0), inputType);
        if (!(collectionType instanceof ArrayType)) {
            return null;
        }
        LogicalType elementType = ((ArrayType) collectionType).getElementType();
        LogicalType needleType = logicalType(operands.get(1), inputType);
        if (needleType == null || needleType.isNullable()) {
            return null;
        }
        Expression array =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, collectionType);
        Expression needle = StreamFusionProjectionTranslator.projectionExpression(
                operands.get(1), inputType, elementType.copy(false));
        return array == null || needle == null
                ? null
                : Expression.newBuilder()
                        .setArrayContains(
                                ArrayContains.newBuilder().setArray(array).setNeedle(needle))
                        .build();
    }

    static Expression arrayReverse(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_REVERSE".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) {
            return null;
        }
        LogicalType operandType = logicalType(operands.get(0), inputType);
        if (!(operandType instanceof ArrayType)
                || !operandType.copy(expectedType.isNullable()).equals(expectedType)) {
            return null;
        }
        Expression array =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, operandType);
        return array == null
                ? null
                : Expression.newBuilder()
                        .setArrayReverse(ArrayReverse.newBuilder().setArray(array))
                        .build();
    }

    static Expression arrayAppend(Object expression, RowType inputType, LogicalType expectedType) {
        java.util.List<?> operands = collectionAndElementOperands(expression, "ARRAY_APPEND", expectedType);
        if (operands == null) {
            return null;
        }
        Expression array = collectionOperand(operands.get(0), inputType);
        Expression element = collectionElementOperand(operands.get(0), operands.get(1), inputType, expectedType);
        return array == null || element == null
                ? null
                : Expression.newBuilder()
                        .setArrayAppend(ArrayAppend.newBuilder().setArray(array).setElement(element))
                        .build();
    }

    static Expression arrayPrepend(Object expression, RowType inputType, LogicalType expectedType) {
        java.util.List<?> operands = collectionAndElementOperands(expression, "ARRAY_PREPEND", expectedType);
        if (operands == null) {
            return null;
        }
        Expression array = collectionOperand(operands.get(0), inputType);
        Expression element = collectionElementOperand(operands.get(0), operands.get(1), inputType, expectedType);
        return array == null || element == null
                ? null
                : Expression.newBuilder()
                        .setArrayPrepend(
                                ArrayPrepend.newBuilder().setArray(array).setElement(element))
                        .build();
    }

    static Expression arrayConcat(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_CONCAT".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() < 2) {
            return null;
        }
        ArrayConcat.Builder concat = ArrayConcat.newBuilder();
        for (Object operand : operands) {
            // Flink's ARRAY_CONCAT type inference has already coerced every operand to
            // the resolved common result type. Calcite does not expose a convertible
            // logical type for every collection-valued call (notably nested calls), so
            // use that authoritative common type while recursively translating them.
            Expression array = StreamFusionProjectionTranslator.projectionExpression(operand, inputType, expectedType);
            if (array == null) {
                return null;
            }
            concat.addArrays(array);
        }
        return Expression.newBuilder().setArrayConcat(concat).build();
    }

    static Expression arrayPosition(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_POSITION".equals(functionName(expression))
                || expectedType.getTypeRoot() != LogicalTypeRoot.INTEGER) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        LogicalType arrayType = logicalType(operands.get(0), inputType);
        LogicalType needleType = logicalType(operands.get(1), inputType);
        if (!(arrayType instanceof ArrayType) || needleType == null) {
            return null;
        }
        LogicalType elementType = ((ArrayType) arrayType).getElementType();
        Expression array = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, arrayType);
        Expression needle =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(1), inputType, elementType);
        return array == null || needle == null
                ? null
                : Expression.newBuilder()
                        .setArrayPosition(
                                ArrayPosition.newBuilder().setArray(array).setNeedle(needle))
                        .build();
    }

    static Expression arrayRemove(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_REMOVE".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        LogicalType arrayType = logicalType(operands.get(0), inputType);
        LogicalType needleType = logicalType(operands.get(1), inputType);
        if (!(arrayType instanceof ArrayType) || needleType == null || needleType.isNullable()) {
            return null;
        }
        LogicalType elementType = ((ArrayType) arrayType).getElementType();
        Expression array = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, arrayType);
        Expression needle = StreamFusionProjectionTranslator.projectionExpression(
                operands.get(1), inputType, elementType.copy(false));
        return array == null || needle == null
                ? null
                : Expression.newBuilder()
                        .setArrayRemove(ArrayRemove.newBuilder().setArray(array).setNeedle(needle))
                        .build();
    }

    static Expression arrayExtremum(Object expression, RowType inputType, LogicalType expectedType) {
        String function = functionName(expression);
        if (!"ARRAY_MIN".equals(function) && !"ARRAY_MAX".equals(function)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1 || !supportsArrayExtremum(expectedType.getTypeRoot())) {
            return null;
        }
        LogicalType arrayType = logicalType(operands.get(0), inputType);
        if (!(arrayType instanceof ArrayType)
                || ((ArrayType) arrayType).getElementType().getTypeRoot() != expectedType.getTypeRoot()) {
            return null;
        }
        Expression array = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, arrayType);
        if (array == null) {
            return null;
        }
        return "ARRAY_MIN".equals(function)
                ? Expression.newBuilder()
                        .setArrayMinimum(ArrayMinimum.newBuilder().setArray(array))
                        .build()
                : Expression.newBuilder()
                        .setArrayMaximum(ArrayMaximum.newBuilder().setArray(array))
                        .build();
    }

    static Expression arrayJoin(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_JOIN".equals(functionName(expression)) || expectedType.getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() < 2 || operands.size() > 3) {
            return null;
        }
        LogicalType arrayType = logicalType(operands.get(0), inputType);
        if (!(arrayType instanceof ArrayType)
                || ((ArrayType) arrayType).getElementType().getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        String delimiter = literal(operands.get(1), String.class);
        if (delimiter == null) {
            return null;
        }
        String replacement = operands.size() == 3 ? literal(operands.get(2), String.class) : null;
        if (operands.size() == 3 && replacement == null) {
            return null;
        }
        Expression array = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, arrayType);
        if (array == null) {
            return null;
        }
        ArrayJoin.Builder join = ArrayJoin.newBuilder().setArray(array).setDelimiter(stringLiteral(delimiter));
        if (replacement != null) {
            join.setNullReplacement(stringLiteral(replacement));
        }
        return Expression.newBuilder().setArrayJoin(join).build();
    }

    static Expression split(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"SPLIT".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        ArrayType resultType = (ArrayType) expectedType;
        if (resultType.getElementType().getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() != 2) {
            return null;
        }
        LogicalType valueType = logicalType(operands.get(0), inputType);
        String delimiter = literal(operands.get(1), String.class);
        if (valueType == null
                || valueType.getTypeRoot() != LogicalTypeRoot.VARCHAR
                || delimiter == null
                || delimiter.isEmpty()) {
            return null;
        }
        Expression value = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, valueType);
        return value == null
                ? null
                : Expression.newBuilder()
                        .setSplit(Split.newBuilder().setValue(value).setDelimiter(stringLiteral(delimiter)))
                        .build();
    }

    static Expression arraySort(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_SORT".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.isEmpty() || operands.size() > 3) {
            return null;
        }
        LogicalType arrayType = logicalType(operands.get(0), inputType);
        if (!(arrayType instanceof ArrayType)
                || !supportsArrayOrdering(
                        ((ArrayType) arrayType).getElementType().getTypeRoot())) {
            return null;
        }
        Boolean ascending = operands.size() >= 2 ? literal(operands.get(1), Boolean.class) : Boolean.TRUE;
        Boolean nullFirst = operands.size() == 3 ? literal(operands.get(2), Boolean.class) : ascending;
        if (ascending == null || nullFirst == null) {
            return null;
        }
        Expression array = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, arrayType);
        return array == null
                ? null
                : Expression.newBuilder()
                        .setArraySort(ArraySort.newBuilder()
                                .setArray(array)
                                .setAscending(ascending)
                                .setNullFirst(nullFirst))
                        .build();
    }

    static Expression arraySlice(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ARRAY_SLICE".equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.size() < 2 || operands.size() > 3) {
            return null;
        }
        LogicalType arrayType = logicalType(operands.get(0), inputType);
        Integer start = literal(operands.get(1), Integer.class);
        Integer end = operands.size() == 3 ? literal(operands.get(2), Integer.class) : null;
        if (!(arrayType instanceof ArrayType) || start == null || (operands.size() == 3 && end == null)) {
            return null;
        }
        Expression array = StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, arrayType);
        if (array == null) {
            return null;
        }
        ArraySlice.Builder slice = ArraySlice.newBuilder().setArray(array).setStart(start.longValue());
        if (end != null) {
            slice.setEnd(end.longValue());
        }
        return Expression.newBuilder().setArraySlice(slice).build();
    }

    private static Expression stringLiteral(String value) {
        return Expression.newBuilder()
                .setStringLiteral(tech.streamfusion.proto.plan.v1.StringLiteral.newBuilder()
                        .setValue(value))
                .build();
    }

    private static boolean supportsArrayExtremum(LogicalTypeRoot type) {
        return type == LogicalTypeRoot.TINYINT
                || type == LogicalTypeRoot.SMALLINT
                || type == LogicalTypeRoot.INTEGER
                || type == LogicalTypeRoot.BIGINT
                || type == LogicalTypeRoot.DECIMAL
                || type == LogicalTypeRoot.VARCHAR
                || type == LogicalTypeRoot.DATE;
    }

    private static boolean supportsArrayOrdering(LogicalTypeRoot type) {
        return supportsArrayExtremum(type);
    }

    private static java.util.List<?> collectionAndElementOperands(
            Object expression, String function, LogicalType expectedType) {
        if (!function.equals(functionName(expression)) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        return operands.size() == 2 ? operands : null;
    }

    private static Expression collectionOperand(Object operand, RowType inputType) {
        LogicalType operandType = logicalType(operand, inputType);
        return operandType instanceof ArrayType
                ? StreamFusionProjectionTranslator.projectionExpression(operand, inputType, operandType)
                : null;
    }

    private static Expression collectionElementOperand(
            Object arrayOperand, Object elementOperand, RowType inputType, LogicalType expectedType) {
        LogicalType arrayType = logicalType(arrayOperand, inputType);
        if (!(arrayType instanceof ArrayType) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        LogicalType expectedElement = ((ArrayType) expectedType).getElementType();
        return StreamFusionProjectionTranslator.projectionExpression(elementOperand, inputType, expectedElement);
    }
}
