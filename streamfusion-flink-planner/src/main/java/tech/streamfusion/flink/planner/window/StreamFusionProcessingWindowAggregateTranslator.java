/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner.window;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Direct UTC processing-time TUMBLE uses the shared DataFusion buffer and ordered slice store. */
public final class StreamFusionProcessingWindowAggregateTranslator {
    private StreamFusionProcessingWindowAggregateTranslator() {}

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
                true,
                "UTC",
                properties);
        try {
            return NativePlan.parseFrom(bytes).toBuilder()
                    .setProtocolVersion(3)
                    .build()
                    .toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Invalid internally generated processing-time window plan", failure);
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
        if (!strategy.isProctime()
                || !(strategy instanceof TimeAttributeWindowingStrategy)
                || !(strategy.getWindow() instanceof TumblingWindowSpec))
            return "processing-time window: verified shared execution requires direct TUMBLE";
        var direct = (TimeAttributeWindowingStrategy) strategy;
        int time = direct.getTimeAttributeIndex();
        // The original window strategy retains PROCTIME ownership. Physical Arrow schemas
        // normalize timestamp kinds after selection and still carry the nullable LTZ(3) slot.
        if (!(strategy.getTimeAttributeType() instanceof LocalZonedTimestampType)
                || ((LocalZonedTimestampType) strategy.getTimeAttributeType()).getPrecision() != 3
                || time < 0
                || time >= input.getFieldCount()
                || !(input.getTypeAt(time) instanceof LocalZonedTimestampType)
                || ((LocalZonedTimestampType) input.getTypeAt(time)).getPrecision() != 3)
            return "processing-time window: input requires a TIMESTAMP_LTZ(3) PROCTIME attribute";
        if (!TableConfigUtils.getLocalTimeZone(config).normalized().equals(java.time.ZoneOffset.UTC))
            return "processing-time window: non-UTC window assignment requires separate clock/zone parity";
        if (retractable) return "processing-time window changelog: shared windows require append-only input";
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED))
            return "processing-time window admission: mini-batch topology remains unverified for production";
        String common = StreamFusionWindowAggregateTranslator.unsupportedReason(
                input, output, keys, calls, strategy, properties, retractable, config);
        if (common != null) return common;
        String metrics = NativeStateMetricSupport.unsupportedReason(config);
        if (metrics != null) return metrics;
        if (StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()).sizeMillis <= 0)
            return "processing-time window: size must be positive";
        if (keys.length != 1 || input.getTypeAt(keys[0]).getTypeRoot() != LogicalTypeRoot.BIGINT)
            return "processing-time window admission: one nullable or non-null BIGINT partition key has verified recovery; other grouping shapes remain gated";
        if (calls.length == 0)
            return "processing-time window admission: DISTINCT-only processing-time windows remain unverified";
        for (int index = 0; index < calls.length; index++) {
            var call = calls[index];
            if (call.getAggregation().getKind() != SqlKind.COUNT
                    || !call.getArgList().isEmpty()
                    || call.isDistinct()
                    || call.filterArg >= 0)
                return "processing-time window aggregate[" + index
                        + "]: verified shared execution requires unfiltered COUNT(*)";
        }
        for (int index = 0; index < properties.length; index++) {
            var property = properties[index].getProperty();
            if (!(property instanceof WindowStart) && !(property instanceof WindowEnd))
                return "processing-time window property[" + index
                        + "]: only start and end properties have verified clock parity";
        }
        return null;
    }
}
