/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.WindowKind;

/** Buffered local-window fragments; capacity is supplied separately from Flink task resources. */
public final class StreamFusionLocalWindowAggregateTranslator {
    private StreamFusionLocalWindowAggregateTranslator() {}

    public static byte[] createStagePlan(
            RowType input,
            RowType output,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            boolean retractable,
            ReadableConfig config) {
        String reason = unsupportedStageReason(input, output, grouping, calls, strategy, retractable, config);
        if (reason != null) throw new IllegalArgumentException(reason);
        boolean attached = strategy instanceof WindowAttachedWindowingStrategy;
        byte[] bytes = StreamFusionWindowAggregatePlan.createLocal(
                input,
                output,
                grouping,
                calls,
                false,
                false,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                attached ? 0 : ((TimeAttributeWindowingStrategy) strategy).getTimeAttributeIndex(),
                // Flink's WindowedSliceAssigner ignores any attached start, even when present.
                -1,
                attached ? ((WindowAttachedWindowingStrategy) strategy).getWindowEnd() : -1,
                "UTC");
        try {
            return NativePlan.parseFrom(bytes).toBuilder()
                    .setProtocolVersion(2)
                    .build()
                    .toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Invalid internally generated local window plan", failure);
        }
    }

    public static String unsupportedStageReason(
            RowType input,
            RowType output,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            boolean retractable,
            ReadableConfig config) {
        if (!(strategy instanceof TimeAttributeWindowingStrategy)
                && !(strategy instanceof WindowAttachedWindowingStrategy))
            return "window strategy: buffered local execution requires direct or attached HOP windows";
        if (strategy.isProctime()
                || !"UTC"
                        .equals(TimeWindowUtil.getShiftTimeZone(
                                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                                .getId())) return "window time: buffered local execution requires UTC event time";
        if (retractable) return "changelog: buffered local windows require append-only input";
        try {
            var window = StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow());
            if (window.kind != WindowKind.WINDOW_KIND_HOP
                    || window.sizeMillis <= 0
                    || window.slideOrStepMillis <= 0
                    || window.sizeMillis % window.slideOrStepMillis != 0)
                return "window: buffered local execution requires an integral HOP size/slide";
        } catch (IllegalArgumentException failure) {
            return "window: " + failure.getMessage();
        }
        int time = strategy instanceof WindowAttachedWindowingStrategy
                ? ((WindowAttachedWindowingStrategy) strategy).getWindowEnd()
                : ((TimeAttributeWindowingStrategy) strategy).getTimeAttributeIndex();
        if (time < 0
                || time >= input.getFieldCount()
                || !(input.getTypeAt(time) instanceof TimestampType)
                || input.getTypeAt(time).isNullable()
                || ((TimestampType) input.getTypeAt(time)).getPrecision() != 3)
            return "window time: buffered local execution requires a non-null TIMESTAMP(3) input";
        for (int index = 0; index < input.getFieldCount(); index++)
            if (!fixedWidth(input.getTypeAt(index)))
                return "input[" + index + "]: buffered local window Flink row-size geometry is not verified for "
                        + input.getTypeAt(index);
        int keys = grouping.length;
        if (output.getFieldCount() != keys + 3)
            return "schema: local window output requires keys, an opaque partial and two bounds";
        for (int index = 0; index < keys; index++)
            if (grouping[index] < 0
                    || grouping[index] >= input.getFieldCount()
                    || !input.getTypeAt(grouping[index]).equals(output.getTypeAt(index)))
                return "key[" + index + "]: local input and output types must match exactly";
        if (output.getTypeAt(keys).getTypeRoot() != LogicalTypeRoot.VARBINARY
                || output.getTypeAt(keys).isNullable())
            return "schema: local window accumulator must be non-null VARBINARY";
        for (int index = keys + 1; index < keys + 3; index++)
            if (output.getTypeAt(index).getTypeRoot() != LogicalTypeRoot.BIGINT
                    || output.getTypeAt(index).isNullable())
                return "schema: local window bounds must be non-null BIGINT";
        for (int index = 0; index < calls.length; index++) {
            var call = calls[index];
            var kind = call.getAggregation().getKind();
            if (call.isDistinct() || (kind != SqlKind.COUNT && kind != SqlKind.MIN && kind != SqlKind.MAX))
                return "aggregate[" + index
                        + "]: buffered local windows require non-DISTINCT COUNT or DataFusion extrema";
            String reason = StreamFusionGroupAggregateTranslator.unsupportedCall(
                    input, FlinkTypeFactory.toLogicalType(call.getType()), call);
            if (reason != null) return "aggregate[" + index + "]: " + reason;
            if (kind == SqlKind.MIN || kind == SqlKind.MAX) {
                var type = input.getTypeAt(call.getArgList().get(0)).getTypeRoot();
                if (type == LogicalTypeRoot.FLOAT || type == LogicalTypeRoot.DOUBLE || type == LogicalTypeRoot.BOOLEAN)
                    return "aggregate[" + index + "]: DataFusion grouped extrema do not preserve this Flink type: "
                            + type;
            }
        }
        return null;
    }

    private static boolean fixedWidth(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) type).getPrecision() == 3;
            default:
                return false;
        }
    }
}
