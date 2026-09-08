/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.window;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;
import tech.streamfusion.flink.planner.aggregate.StreamFusionGroupAggregateTranslator;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.WindowKind;

/** Global-window fragment construction; the common native region owns its runtime, state and controls. */
public final class StreamFusionGlobalWindowAggregateTranslator {
    private StreamFusionGlobalWindowAggregateTranslator() {}

    public static byte[] createStagePlan(
            RowType originalInput,
            RowType partialInput,
            RowType output,
            int keys,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean retractable,
            ReadableConfig config) {
        String reason = unsupportedStageReason(
                originalInput, partialInput, output, keys, calls, strategy, properties, retractable, config);
        if (reason != null) throw new IllegalArgumentException(reason);
        byte[] bytes = StreamFusionWindowAggregatePlan.createGlobal(
                originalInput,
                partialInput,
                output,
                keys,
                calls,
                false,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                strategy instanceof TimeAttributeWindowingStrategy,
                shiftTimeZone(strategy, config),
                properties);
        try {
            return NativePlan.parseFrom(bytes).toBuilder()
                    .setProtocolVersion(2)
                    .build()
                    .toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Invalid internally generated global window plan", failure);
        }
    }

    public static String unsupportedStageReason(
            RowType originalInput,
            RowType partialInput,
            RowType output,
            int keys,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean retractable,
            ReadableConfig config) {
        if (!(strategy instanceof TimeAttributeWindowingStrategy)
                && !(strategy instanceof WindowAttachedWindowingStrategy))
            return "window strategy: shared global execution requires direct or attached HOP windows";
        if (strategy.isProctime() || !"UTC".equals(shiftTimeZone(strategy, config)))
            return "window time: shared global execution requires UTC event time";
        if (retractable) return "changelog: shared global window partials must be append-only";
        try {
            var window = StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow());
            if (window.kind != WindowKind.WINDOW_KIND_HOP
                    || window.sizeMillis <= 0
                    || window.slideOrStepMillis <= 0
                    || window.sizeMillis % window.slideOrStepMillis != 0)
                return "window: shared global execution requires an integral HOP size/slide";
        } catch (IllegalArgumentException unsupported) {
            return "window: " + unsupported.getMessage();
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED))
            return "state: shared windows require synchronous Flink state";
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG))
            return "state: Flink changelog-state wrapping is not implemented by shared windows";
        String metricReason = NativeStateMetricSupport.unsupportedReason(config);
        if (metricReason != null) return metricReason;
        if (keys < 0
                || partialInput.getFieldCount() != keys + 3
                || output.getFieldCount() != keys + calls.length + properties.length)
            return "schema: global windows require keys, an opaque partial and two bounds; output requires keys, aggregates and properties";
        if (partialInput.getTypeAt(keys).getTypeRoot() != LogicalTypeRoot.VARBINARY
                || partialInput.getTypeAt(keys).isNullable())
            return "schema: global window accumulator must be non-null VARBINARY";
        for (int index = keys + 1; index < keys + 3; index++)
            if (partialInput.getTypeAt(index).getTypeRoot() != LogicalTypeRoot.BIGINT
                    || partialInput.getTypeAt(index).isNullable())
                return "schema: global window bounds must be non-null BIGINT";
        for (int index = 0; index < keys; index++)
            if (!partialInput.getTypeAt(index).equals(output.getTypeAt(index)))
                return "key[" + index + "]: global input and output types must match exactly";
        for (int index = 0; index < calls.length; index++) {
            AggregateCall call = calls[index];
            SqlKind kind = call.getAggregation().getKind();
            if (call.isDistinct() || (kind != SqlKind.COUNT && kind != SqlKind.MIN && kind != SqlKind.MAX))
                return "aggregate[" + index
                        + "]: shared global windows require non-DISTINCT COUNT or DataFusion append-only extrema";
            String reason = StreamFusionGroupAggregateTranslator.unsupportedCall(
                    originalInput, output.getTypeAt(keys + index), call);
            if (reason != null) return "aggregate[" + index + "]: " + reason;
            if (kind == SqlKind.MIN || kind == SqlKind.MAX) {
                var type = originalInput.getTypeAt(call.getArgList().get(0)).getTypeRoot();
                if (type == LogicalTypeRoot.FLOAT || type == LogicalTypeRoot.DOUBLE || type == LogicalTypeRoot.BOOLEAN)
                    return "aggregate[" + index + "]: DataFusion grouped extrema do not preserve this Flink type: "
                            + type;
            }
        }
        for (int index = 0; index < properties.length; index++) {
            Object property = properties[index].getProperty();
            if (!(property instanceof WindowStart)
                    && !(property instanceof WindowEnd)
                    && !(property instanceof RowtimeAttribute))
                return "window property[" + index + "]: shared windows require event-time start, end or rowtime";
        }
        return null;
    }

    private static String shiftTimeZone(WindowingStrategy strategy, ReadableConfig config) {
        return TimeWindowUtil.getShiftTimeZone(
                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
    }
}
