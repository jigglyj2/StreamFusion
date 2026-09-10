/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** DataFusion's bytewise string extrema require Flink's serialized-input comparison semantics. */
final class StreamFusionStringAggregateAdmission {
    private StreamFusionStringAggregateAdmission() {}

    static boolean isStringExtremum(AggregateCall call) {
        var kind = call.getAggregation().getKind();
        return (kind == SqlKind.MIN || kind == SqlKind.MAX) && call.getType().getSqlTypeName() == SqlTypeName.VARCHAR;
    }

    static String unsupportedReason(StreamExecGroupAggregate aggregate, AggregateCall call, RowType input) {
        if (call.isDistinct()
                || call.getArgList().size() != 1
                || input.getTypeAt(call.getArgList().get(0)).getTypeRoot() != LogicalTypeRoot.VARCHAR)
            return "VARCHAR MIN/MAX admission: only a non-DISTINCT VARCHAR argument is verified";
        if (FlinkExecNodeAccess.aggregateBooleanField(aggregate, "needRetraction"))
            return "VARCHAR MIN/MAX admission: retractable string extrema remain unverified; input must be append-only";
        // BinaryStringData.compareTo uses UTF-16 only when both values cache Java strings.
        // StreamExecExchange.HASH installs a non-chainable KeyGroupStreamPartitioner, so
        // each incoming value has crossed Flink's RowData serializer and uses UTF-8 order.
        // A Calc after that exchange can recreate Java-backed strings; do not look through it.
        var source = aggregate.getInputEdges().get(0).getSource();
        if (!(source instanceof StreamExecExchange)
                || source.getInputProperties().get(0).getRequiredDistribution().getType()
                        != InputProperty.DistributionType.HASH)
            return "VARCHAR MIN/MAX comparison: input must directly consume a HASH exchange to preserve "
                    + "Flink binary-string ordering; Java-backed string ordering is not equivalent";
        return null;
    }
}
