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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StructField;

/** Complex-type expressions kept separate from the scalar Calc translator. */
final class StreamFusionComplexProjectionTranslator extends StreamFusionRexSupport {
    private StreamFusionComplexProjectionTranslator() {}

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
        int inputIndex = inputIndex(base);
        if (inputIndex < 0 || inputIndex >= inputType.getFieldCount()) {
            return null;
        }

        LogicalType currentType = inputType.getTypeAt(inputIndex);
        Expression current = StreamFusionIdentityCalcOperator.inputReference(
                inputIndex, StreamFusionIdentityCalcOperator.logicalType(currentType));
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
}
