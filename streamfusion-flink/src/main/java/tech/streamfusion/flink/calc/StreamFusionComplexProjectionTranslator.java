/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion.flink.calc;

import java.util.LinkedList;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.ArrayElement;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StructField;

/** Complex-type expressions kept separate from the scalar Calc translator. */
final class StreamFusionComplexProjectionTranslator extends StreamFusionRexSupport {
    private StreamFusionComplexProjectionTranslator() {}

    static Expression arrayElement(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"ITEM"
                .equals(
                        hasNoArgMethod(expression, "getKind")
                                ? invoke(expression, "getKind").toString()
                                : "")) {
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
        if (!elementType.copy(expectedType.isNullable()).equals(expectedType)) {
            return null;
        }
        Integer index = integerLiteral(operands.get(1));
        if (index == null || index <= 0) {
            return null;
        }
        Expression array =
                StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, collectionType);
        return array == null
                ? null
                : Expression.newBuilder()
                        .setArrayElement(
                                ArrayElement.newBuilder().setArray(array).setIndex(index))
                        .build();
    }

    static Expression structField(Object expression, RowType inputType, LogicalType expectedType) {
        if (!"RexFieldAccess".equals(expression.getClass().getSimpleName())) {
            return null;
        }

        LinkedList<Object> fields = new LinkedList<>();
        Object base = expression;
        while ("RexFieldAccess".equals(base.getClass().getSimpleName())) {
            fields.addFirst(invoke(base, "getField"));
            base = invoke(base, "getReferenceExpr");
        }
        LogicalType currentType = logicalType(base, inputType);
        if (currentType == null) {
            return null;
        }
        Expression current = StreamFusionProjectionTranslator.projectionExpression(base, inputType, currentType);
        if (current == null) {
            return null;
        }
        for (Object field : fields) {
            if (!(currentType instanceof RowType)) {
                return null;
            }
            RowType rowType = (RowType) currentType;
            int fieldIndex = (int) invoke(field, "getIndex");
            if (fieldIndex < 0 || fieldIndex >= rowType.getFieldCount()) {
                return null;
            }
            RowType.RowField rowField = rowType.getFields().get(fieldIndex);
            current = Expression.newBuilder()
                    .setStructField(StructField.newBuilder().setOperand(current).setFieldName(rowField.getName()))
                    .build();
            currentType = rowField.getType();
        }
        return currentType.copy(expectedType.isNullable()).equals(expectedType) ? current : null;
    }

    private static LogicalType logicalType(Object expression, RowType inputType) {
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
