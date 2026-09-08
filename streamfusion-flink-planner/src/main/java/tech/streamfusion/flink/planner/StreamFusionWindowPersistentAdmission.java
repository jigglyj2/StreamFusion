/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Verified two-phase window state subset; fragment lowering still enforces time and backend semantics. */
final class StreamFusionWindowPersistentAdmission {
    private StreamFusionWindowPersistentAdmission() {}

    static String unsupportedReason(ExecNode<?> node, ReadableConfig activeConfig) {
        StreamExecLocalWindowAggregate local;
        if (node instanceof StreamExecGlobalWindowAggregate) {
            var pair = twoPhaseWindowAggregate((StreamExecGlobalWindowAggregate) node);
            if (pair == null)
                return "window persistent admission: global windows require a matching local/exchange stage";
            local = pair.local;
        } else local = (StreamExecLocalWindowAggregate) node;
        var config = activeConfig == null ? new Configuration() : Configuration.fromMap(activeConfig.toMap());
        config.addAll(Configuration.fromMap(
                ((ExecNodeBase<?>) node).getPersistedConfig().toMap()));
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED))
            return "window persistent admission: mini-batch topology remains unverified for production";
        var input = (RowType) local.getInputEdges().get(0).getOutputType();
        for (int key : localWindowGrouping(local)) {
            var type = input.getTypeAt(key).getTypeRoot();
            if (type != LogicalTypeRoot.BIGINT && type != LogicalTypeRoot.INTEGER)
                return "window persistent admission: grouping type " + type
                        + " lacks the complete metric/recovery proof for production";
        }
        var calls = localWindowAggregateCalls(local);
        if (calls.length == 0) return "window persistent admission: DISTINCT-only windows remain unverified";
        for (var call : calls) {
            var kind = call.getAggregation().getKind();
            if (call.isDistinct()
                    || call.getType().getSqlTypeName() != SqlTypeName.BIGINT
                    || (kind != SqlKind.COUNT && kind != SqlKind.MIN && kind != SqlKind.MAX))
                return "window persistent admission: verified calls are non-DISTINCT BIGINT COUNT/MIN/MAX";
            for (int argument : call.getArgList())
                if (input.getTypeAt(argument).getTypeRoot() != LogicalTypeRoot.BIGINT)
                    return "window persistent admission: aggregate argument type " + input.getTypeAt(argument)
                            + " lacks the complete metric/recovery proof for production";
        }
        return null;
    }
}
