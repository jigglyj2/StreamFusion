/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.planner.window.StreamFusionSessionWindowAggregateTranslator;

/** Single source of production and fragment admission for verified shared session windows. */
final class StreamFusionSessionWindowAdmission {
    private StreamFusionSessionWindowAdmission() {}

    static String unsupportedReason(StreamExecWindowAggregate node, ReadableConfig activeConfig) {
        var config = activeConfig == null ? new Configuration() : Configuration.fromMap(activeConfig.toMap());
        config.addAll(Configuration.fromMap(node.getPersistedConfig().toMap()));
        return StreamFusionSessionWindowAggregateTranslator.unsupportedReason(
                (RowType) node.getInputEdges().get(0).getOutputType(),
                (RowType) node.getOutputType(),
                windowGrouping(node),
                windowAggregateCalls(node),
                windowing(node),
                windowProperties(node),
                windowNeedRetraction(node),
                config);
    }
}
