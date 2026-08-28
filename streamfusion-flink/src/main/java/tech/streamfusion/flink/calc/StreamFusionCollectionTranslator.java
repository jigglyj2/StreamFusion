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
import tech.streamfusion.proto.plan.v1.ArrayContains;
import tech.streamfusion.proto.plan.v1.ArrayPrepend;
import tech.streamfusion.proto.plan.v1.ArrayReverse;
import tech.streamfusion.proto.plan.v1.Cardinality;
import tech.streamfusion.proto.plan.v1.Expression;

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
