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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.RowConstructor;

/** Translates Flink's typed {@code ROW(...)} value constructor. */
final class StreamFusionRowConstructorTranslator extends StreamFusionRexSupport {
    private StreamFusionRowConstructorTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!isRowConstructor(expression) || !(expectedType instanceof RowType)) {
            return null;
        }
        RowType rowType = (RowType) expectedType;
        java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
        if (operands.isEmpty() || operands.size() != rowType.getFieldCount()) {
            return null;
        }
        RowConstructor.Builder constructor = RowConstructor.newBuilder();
        for (int index = 0; index < operands.size(); index++) {
            LogicalType fieldType = rowType.getTypeAt(index);
            Expression field =
                    StreamFusionProjectionTranslator.projectionExpression(operands.get(index), inputType, fieldType);
            if (field == null) {
                return null;
            }
            constructor.addFieldNames(rowType.getFieldNames().get(index)).addFields(field);
        }
        return Expression.newBuilder().setRowConstructor(constructor).build();
    }

    static String failureReason(Object expression) {
        return isRowConstructor(expression) && ((java.util.List<?>) invoke(expression, "getOperands")).isEmpty()
                ? "empty ROW constructors stay on Flink because DataFusion named structs require at least one field"
                : null;
    }

    private static boolean isRowConstructor(Object expression) {
        return hasNoArgMethod(expression, "getKind")
                && "ROW".equals(invoke(expression, "getKind").toString());
    }
}
