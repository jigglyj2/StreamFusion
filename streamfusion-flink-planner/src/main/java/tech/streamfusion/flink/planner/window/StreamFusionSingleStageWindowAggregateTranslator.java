/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner.window;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.types.logical.RowType;

/** Selects the verified shared single-stage implementation without changing Flink's time strategy. */
public final class StreamFusionSingleStageWindowAggregateTranslator {
    private StreamFusionSingleStageWindowAggregateTranslator() {}

    public static byte[] createStagePlan(
            RowType input,
            RowType output,
            int[] keys,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean retractable,
            ReadableConfig config) {
        return strategy.isProctime()
                ? StreamFusionProcessingWindowAggregateTranslator.createStagePlan(
                        input, output, keys, calls, strategy, properties, retractable, config)
                : StreamFusionSessionWindowAggregateTranslator.createStagePlan(
                        input, output, keys, calls, strategy, properties, retractable, config);
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
        return strategy.isProctime()
                ? StreamFusionProcessingWindowAggregateTranslator.unsupportedReason(
                        input, output, keys, calls, strategy, properties, retractable, config)
                : StreamFusionSessionWindowAggregateTranslator.unsupportedReason(
                        input, output, keys, calls, strategy, properties, retractable, config);
    }
}
