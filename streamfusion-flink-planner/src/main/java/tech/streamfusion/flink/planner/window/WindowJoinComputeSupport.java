/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner.window;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/** Mirrors the bounded DataFusion candidate kernels in window_join/computation/admission.rs. */
final class WindowJoinComputeSupport {
    private WindowJoinComputeSupport() {}

    static String inputReason(RowType type, int windowEnd) {
        if (windowEnd < 0 || windowEnd >= type.getFieldCount()) return "window join: window-end index outside input";
        LogicalType end = type.getTypeAt(windowEnd);
        if (end.getTypeRoot() != org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT
                && !(end instanceof TimestampType && ((TimestampType) end).getPrecision() == 3))
            return "window join: window-end columns require BIGINT or TIMESTAMP(3)";
        for (LogicalType field : type.getChildren()) {
            switch (field.getTypeRoot()) {
                case NULL:
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                    break;
                default:
                    return "window join: bounded candidate workspace currently requires scalar payloads; unsupported "
                            + field;
            }
        }
        return null;
    }

    static boolean boundedPredicate(RexNode expression) {
        if (expression instanceof RexInputRef || expression instanceof RexLiteral) return true;
        if (!(expression instanceof RexCall)) return false;
        RexCall call = (RexCall) expression;
        switch (call.getKind()) {
            case AND:
            case OR:
            case NOT:
            case IS_NULL:
            case IS_NOT_NULL:
            case CASE:
                break;
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                // Floating comparisons inject a NaN kernel; mixed decimal comparisons
                // inject widening casts. Neither is in the bounded candidate kernel contract.
                var first = call.getOperands().get(0).getType();
                var second = call.getOperands().get(1).getType();
                if (first.getSqlTypeName() != second.getSqlTypeName()
                        || first.getSqlTypeName() == SqlTypeName.FLOAT
                        || first.getSqlTypeName() == SqlTypeName.REAL
                        || first.getSqlTypeName() == SqlTypeName.DOUBLE
                        || first.getPrecision() != second.getPrecision()
                        || first.getScale() != second.getScale()) return false;
                break;
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case MOD:
                // Decimal result conversion and timestamp offsets lower to different kernels.
                for (RexNode operand : call.getOperands()) {
                    switch (operand.getType().getSqlTypeName()) {
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case FLOAT:
                        case REAL:
                        case DOUBLE:
                            break;
                        default:
                            return false;
                    }
                }
                break;
            default:
                return false;
        }
        return call.getOperands().stream().allMatch(WindowJoinComputeSupport::boundedPredicate);
    }
}
