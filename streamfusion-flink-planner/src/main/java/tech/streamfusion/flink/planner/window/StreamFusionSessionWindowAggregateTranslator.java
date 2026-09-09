/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner.window;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Verified SESSION fragment; Flink owns routing and recovery, the shared native region owns execution. */
public final class StreamFusionSessionWindowAggregateTranslator {
    private StreamFusionSessionWindowAggregateTranslator() {}

    public static byte[] createStagePlan(
            RowType input,
            RowType output,
            int[] keys,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean retractable,
            ReadableConfig config) {
        String reason = unsupportedReason(input, output, keys, calls, strategy, properties, retractable, config);
        if (reason != null) throw new IllegalArgumentException(reason);
        var direct = (TimeAttributeWindowingStrategy) strategy;
        byte[] bytes = StreamFusionWindowAggregatePlan.create(
                input,
                output,
                keys,
                calls,
                false,
                false,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                direct.getTimeAttributeIndex(),
                -1,
                -1,
                false,
                "UTC",
                properties);
        try {
            return NativePlan.parseFrom(bytes).toBuilder()
                    .setProtocolVersion(2)
                    .build()
                    .toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Invalid internally generated session window plan", failure);
        }
    }

    public static String unsupportedReason(
            RowType input,
            RowType output,
            int[] keys,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean retractable,
            ReadableConfig config) {
        if (strategy.isProctime())
            return "window processing time: shared processing-time planner resource binding and production parity are not yet verified";
        if (!(strategy instanceof TimeAttributeWindowingStrategy)
                || !(strategy.getWindow() instanceof SessionWindowSpec))
            return "session persistent admission: shared single-stage windows require direct SESSION event time";
        var direct = (TimeAttributeWindowingStrategy) strategy;
        if (strategy.isProctime()
                || !(strategy.getTimeAttributeType() instanceof TimestampType)
                || ((TimestampType) strategy.getTimeAttributeType()).getPrecision() != 3)
            return "session time: shared sessions require TIMESTAMP(3) event time without time zone";
        int time = direct.getTimeAttributeIndex();
        if (time < 0
                || time >= input.getFieldCount()
                || !(input.getTypeAt(time) instanceof TimestampType)
                || ((TimestampType) input.getTypeAt(time)).getPrecision() != 3)
            return "session time: event-time input must be a TIMESTAMP(3) column";
        if (retractable) return "session changelog: shared sessions require append-only input";
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED))
            return "session persistent admission: mini-batch topology remains unverified for production";
        String common = StreamFusionWindowAggregateTranslator.unsupportedReason(
                input, output, keys, calls, strategy, properties, retractable, config);
        if (common != null) return common;
        String metrics = NativeStateMetricSupport.unsupportedReason(config);
        if (metrics != null) return metrics;
        if (StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()).sizeMillis <= 0)
            return "session window: gap must be positive";
        if (keys.length != 1 || input.getTypeAt(keys[0]).getTypeRoot() != LogicalTypeRoot.BIGINT)
            return "session persistent admission: one nullable or non-null BIGINT partition key has verified recovery; other grouping shapes remain gated";
        if (calls.length == 0) return "session persistent admission: DISTINCT-only sessions remain unverified";
        for (int index = 0; index < calls.length; index++) {
            var call = calls[index];
            if (call.getAggregation().getKind() != SqlKind.COUNT
                    || !call.getArgList().isEmpty()
                    || call.isDistinct()
                    || call.filterArg >= 0)
                return "session aggregate[" + index + "]: verified shared execution requires unfiltered COUNT(*)";
        }
        for (int index = 0; index < properties.length; index++) {
            var property = properties[index].getProperty();
            if (!(property instanceof WindowStart)
                    && !(property instanceof WindowEnd)
                    && !(property instanceof RowtimeAttribute))
                return "session window property[" + index + "]: event-time start, end or rowtime required";
        }
        return null;
    }
}
