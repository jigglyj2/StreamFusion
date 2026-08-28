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
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.ArrayConstructor;
import tech.streamfusion.proto.plan.v1.Expression;

/** Translates Flink's typed {@code ARRAY[...]} value constructor. */
final class StreamFusionArrayConstructorTranslator extends StreamFusionRexSupport {
    private StreamFusionArrayConstructorTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!isArrayConstructor(expression) || !(expectedType instanceof ArrayType)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.isEmpty()) {
            return null;
        }
        LogicalType elementType = ((ArrayType) expectedType).getElementType();
        ArrayConstructor.Builder constructor = ArrayConstructor.newBuilder();
        for (Object operand : operands) {
            Expression element = StreamFusionProjectionTranslator.projectionExpression(operand, inputType, elementType);
            if (element == null) {
                return null;
            }
            constructor.addElements(element);
        }
        return Expression.newBuilder().setArrayConstructor(constructor).build();
    }

    static String failureReason(Object expression) {
        if (!isArrayConstructor(expression)) {
            return null;
        }
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        return operands.isEmpty()
                ? "empty ARRAY constructors stay on Flink until a correctly typed empty Arrow array is encoded"
                : null;
    }

    private static boolean isArrayConstructor(Object expression) {
        return hasNoArgMethod(expression, "getKind")
                && "ARRAY_VALUE_CONSTRUCTOR"
                        .equals(invoke(expression, "getKind").toString());
    }
}
