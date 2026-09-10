/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.window;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Reflection entry point for native event-time Window Join. */
public final class StreamFusionWindowJoinTranslator {
    private StreamFusionWindowJoinTranslator() {}

    /** Builds a binary fragment; the common native region owns exchange, state and execution. */
    public static byte[] createStagePlan(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            WindowingStrategy leftWindowing,
            WindowingStrategy rightWindowing,
            ReadableConfig config) {
        String reason =
                unsupportedReason(leftType, rightType, outputType, joinSpec, leftWindowing, rightWindowing, config);
        if (reason != null) throw new IllegalArgumentException(reason);
        return StreamFusionWindowJoinPlan.createNativeInner(
                leftType,
                rightType,
                joinSpec,
                ((WindowAttachedWindowingStrategy) leftWindowing).getWindowEnd(),
                ((WindowAttachedWindowingStrategy) rightWindowing).getWindowEnd(),
                TimeWindowUtil.getShiftTimeZone(
                                leftWindowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                        .getId());
    }

    public static String unsupportedReason(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            WindowingStrategy leftWindowing,
            WindowingStrategy rightWindowing,
            ReadableConfig config) {
        if (!(leftWindowing instanceof WindowAttachedWindowingStrategy)
                || !(rightWindowing instanceof WindowAttachedWindowingStrategy)) {
            return "window strategy: Window Join requires attached window columns on both inputs";
        }
        if (!leftWindowing.isRowtime() || !rightWindowing.isRowtime()) {
            return "window time: Flink does not support processing-time Window Join";
        }
        if (joinSpec.getJoinType() != FlinkJoinType.INNER) {
            return "window join: native computation currently requires INNER semantics";
        }
        if (!TimeWindowUtil.getShiftTimeZone(
                        leftWindowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId()
                .equals("UTC")) {
            return "window join: native computation currently requires UTC event time";
        }
        if (joinSpec.getLeftKeys().length != joinSpec.getRightKeys().length
                || joinSpec.getLeftKeys().length != joinSpec.getFilterNulls().length) {
            return "join keys: left, right, and null-filter key counts differ";
        }
        for (int key : joinSpec.getLeftKeys()) {
            if (key < 0 || key >= leftType.getFieldCount()) {
                return "left join key: index " + key + " is outside the input row";
            }
        }
        for (int key : joinSpec.getRightKeys()) {
            if (key < 0 || key >= rightType.getFieldCount()) {
                return "right join key: index " + key + " is outside the input row";
            }
        }
        for (int i = 0; i < joinSpec.getLeftKeys().length; i++) {
            LogicalType leftKey = leftType.getTypeAt(joinSpec.getLeftKeys()[i]);
            LogicalType rightKey = rightType.getTypeAt(joinSpec.getRightKeys()[i]);
            if (!leftKey.copy(true).equals(rightKey.copy(true)) || !nativeKey(leftKey)) {
                return "join keys: native Window Join requires matching Flink-compatible scalar equality keys";
            }
        }
        String leftFailure = WindowJoinComputeSupport.inputReason(
                leftType, ((WindowAttachedWindowingStrategy) leftWindowing).getWindowEnd());
        if (leftFailure != null) return leftFailure;
        String rightFailure = WindowJoinComputeSupport.inputReason(
                rightType, ((WindowAttachedWindowingStrategy) rightWindowing).getWindowEnd());
        if (rightFailure != null) return rightFailure;
        if (joinSpec.getNonEquiCondition().isPresent()) {
            String failure = tech.streamfusion.flink.calc.StreamFusionCalcTranslator.operatorConditionFailure(
                    joinSpec.getNonEquiCondition().get(),
                    StreamFusionWindowJoinPlan.conditionInputType(leftType, rightType),
                    "window join condition");
            if (failure != null) return failure;
            if (!WindowJoinComputeSupport.boundedPredicate(
                    joinSpec.getNonEquiCondition().get()))
                return "window join: residual predicate requires bounded candidate workspace; expanding or unsupported kernels are not admitted";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native Window Join";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native Window Join";
        }
        return null;
    }

    private static boolean nativeKey(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case VARCHAR:
            case VARBINARY:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }
}
