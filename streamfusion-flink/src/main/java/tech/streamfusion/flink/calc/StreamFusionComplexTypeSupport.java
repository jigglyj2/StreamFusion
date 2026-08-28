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
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

/** Resolves nested Calcite expression types from Flink's authoritative input schema. */
abstract class StreamFusionComplexTypeSupport extends StreamFusionRexSupport {
    protected static LogicalType logicalType(Object expression, RowType inputType) {
        int inputIndex = inputIndex(expression);
        if (inputIndex >= 0 && inputIndex < inputType.getFieldCount()) {
            return inputType.getTypeAt(inputIndex);
        }
        if ("RexFieldAccess".equals(expression.getClass().getSimpleName())) {
            LogicalType parent = logicalType(invoke(expression, "getReferenceExpr"), inputType);
            int fieldIndex = (int) invoke(invoke(expression, "getField"), "getIndex");
            return parent instanceof RowType && fieldIndex >= 0 && fieldIndex < ((RowType) parent).getFieldCount()
                    ? ((RowType) parent).getTypeAt(fieldIndex)
                    : null;
        }
        String kind = hasNoArgMethod(expression, "getKind")
                ? invoke(expression, "getKind").toString()
                : "";
        if ("ITEM".equals(kind)) {
            java.util.List<?> operands = (java.util.List<?>) invoke(expression, "getOperands");
            if (operands.isEmpty()) {
                return null;
            }
            LogicalType collection = logicalType(operands.get(0), inputType);
            if (collection instanceof ArrayType) {
                return ((ArrayType) collection).getElementType();
            }
            if (collection instanceof MapType) {
                return ((MapType) collection).getValueType();
            }
        }
        return StreamFusionExpressionTranslator.expressionLogicalType(expression);
    }
}
