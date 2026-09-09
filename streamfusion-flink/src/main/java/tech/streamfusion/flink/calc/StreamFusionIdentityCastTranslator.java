/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.calc;

import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.proto.plan.v1.Expression;

/** Casts that Flink implements by forwarding the original value and Arrow can represent unchanged. */
final class StreamFusionIdentityCastTranslator extends StreamFusionRexSupport {
    private StreamFusionIdentityCastTranslator() {}

    static Expression translate(Object expression, RowType inputType, LogicalType expectedType) {
        if (!expression.getClass().getSimpleName().equals("RexCall")
                || !("CAST".equals(invoke(expression, "getKind").toString())
                        || "TRY_CAST".equals(functionName(expression)))) return null;
        List<?> operands = (List<?>) invoke(expression, "getOperands");
        if (operands.size() != 1) return null;
        LogicalType sourceType = StreamFusionExpressionTranslator.expressionLogicalType(operands.get(0));
        LogicalType targetType = StreamFusionExpressionTranslator.expressionLogicalType(expression);
        if (!sameType(targetType, expectedType)) return null;
        if (sameType(sourceType, targetType)) {
            return StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, expectedType);
        }
        // Flink CharVarCharTrimPadCastRule trusts the declared input width and
        // forwards widening VARCHAR casts, without trimming, padding or copying.
        if (sourceType instanceof VarCharType
                && targetType instanceof VarCharType
                && ((VarCharType) sourceType).getLength() <= ((VarCharType) targetType).getLength()) {
            return StreamFusionProjectionTranslator.projectionExpression(operands.get(0), inputType, sourceType);
        }
        return null;
    }

    private static boolean sameType(LogicalType left, LogicalType right) {
        return left != null && right != null && left.copy(true).equals(right.copy(true));
    }
}
