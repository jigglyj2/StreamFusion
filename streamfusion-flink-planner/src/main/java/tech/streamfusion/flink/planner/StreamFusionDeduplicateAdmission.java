/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

/** Row-time extrema with DataFusion compute and shared memory/metric/recovery conformance. */
final class StreamFusionDeduplicateAdmission {
    private StreamFusionDeduplicateAdmission() {}

    static String unsupportedReason(StreamExecDeduplicate node, ReadableConfig activeConfig) {
        var config = activeConfig == null ? new Configuration() : Configuration.fromMap(activeConfig.toMap());
        config.addAll(Configuration.fromMap(node.getPersistedConfig().toMap()));
        if (!booleanField(node, "isRowtime"))
            return "deduplicate persistent admission: processing-time computation and shared recovery remain unverified";
        if (booleanField(node, "outputInsertOnly"))
            return "deduplicate persistent admission: timer-backed row-time insert-only output remains unverified";
        if (stateTtl(node) != 0
                || !config.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).isZero())
            return "deduplicate persistent admission: state TTL is not implemented";
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
                || config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED))
            return "deduplicate persistent admission: verified execution requires synchronous state and disabled mini-batching";
        var input = (RowType) node.getInputEdges().get(0).getOutputType();
        for (var type : input.getChildren()) {
            switch (type.getTypeRoot()) {
                case BIGINT:
                case INTEGER:
                case VARCHAR:
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    if (((TimestampType) type).getPrecision() == 3
                            && ((TimestampType) type).getKind() != TimestampKind.PROCTIME) break;
                default:
                    return "deduplicate persistent admission: input type " + type
                            + " lacks shared conformance; verified types are BIGINT, INTEGER, VARCHAR and TIMESTAMP(3) without processing time";
            }
        }
        return null;
    }
}
