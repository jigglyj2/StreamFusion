/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.aggregateCalls;
import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.grouping;

import java.util.Set;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Verified persistent execution subsets; semantic and backend configuration checks still apply. */
final class StreamFusionPersistentAdmission {
    private static final Set<SqlKind> INTEGER_AGGREGATES =
            Set.of(SqlKind.COUNT, SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX, SqlKind.AVG);

    static String unsupportedReason(ExecNode<?> node, ReadableConfig activeConfig) {
        try {
            if (node instanceof StreamExecMultiJoin) {
                var join = FlinkExecNodeAccess.binaryMultiJoinSpec((StreamExecMultiJoin) node);
                if (join != null) {
                    if (join.getNonEquiCondition()
                            .map(StreamFusionPersistentAdmission::boundedPredicate)
                            .orElse(true)) return null;
                    return "binary join residual workspace: only boolean combinations of direct column/literal "
                            + "comparisons and null checks have verified bounded Arrow workspace; "
                            + "computed predicate operands remain on Flink";
                }
            }
            if (node instanceof StreamExecGroupAggregate)
                return aggregateReason((StreamExecGroupAggregate) node, activeConfig);
        } catch (RuntimeException failure) {
            return "could not verify native persistent execution subset: " + failure.getMessage();
        }
        return "native persistent state is temporarily disabled; large retained-state/buffer admission, "
                + "Flink backend configuration parity, and checkpoint/metric conformance are not yet verified "
                + "for this physical family";
    }

    private static String aggregateReason(StreamExecGroupAggregate aggregate, ReadableConfig activeConfig) {
        var config = activeConfig == null ? new Configuration() : Configuration.fromMap(activeConfig.toMap());
        config.addAll(Configuration.fromMap(aggregate.getPersistedConfig().toMap()));
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED))
            return "aggregate persistent admission: mini-batch bundle lifecycle remains unverified for production";
        var input = (RowType) aggregate.getInputEdges().get(0).getOutputType();
        var keys = grouping(aggregate);
        if (keys.length == 0)
            return "aggregate persistent admission: singleton/global aggregation retains its separate production gate";
        for (int key : keys) {
            var type = input.getTypeAt(key).getTypeRoot();
            if (type != LogicalTypeRoot.BIGINT && type != LogicalTypeRoot.INTEGER && type != LogicalTypeRoot.VARCHAR)
                return "aggregate persistent admission: grouping type " + type
                        + " lacks the complete metric/recovery proof for production";
        }
        var calls = aggregateCalls(aggregate);
        if (calls.length == 0)
            return "aggregate persistent admission: SELECT DISTINCT retains its separate production gate";
        for (var call : calls) {
            if (!INTEGER_AGGREGATES.contains(call.getAggregation().getKind())
                    || call.isDistinct()
                    || call.getType().getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.BIGINT)
                return "aggregate persistent admission: verified calls are non-DISTINCT BIGINT COUNT/SUM/SUM0/MIN/MAX/AVG";
            for (int index : call.getArgList())
                if (input.getTypeAt(index).getTypeRoot() != LogicalTypeRoot.BIGINT)
                    return "aggregate persistent admission: aggregate argument type " + input.getTypeAt(index)
                            + " lacks the complete metric/recovery proof for production";
        }
        return null;
    }

    static boolean boundedPredicate(RexNode expression) {
        if (expression.getKind() == SqlKind.LITERAL) return true;
        if (!(expression instanceof RexCall)) return false;
        var call = (RexCall) expression;
        switch (call.getKind()) {
            case AND:
            case OR:
            case NOT:
                return call.getOperands().stream().allMatch(StreamFusionPersistentAdmission::boundedPredicate);
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IS_NULL:
            case IS_NOT_NULL:
                return call.getOperands().stream()
                        .allMatch(operand ->
                                operand.getKind() == SqlKind.INPUT_REF || operand.getKind() == SqlKind.LITERAL);
            default:
                return false;
        }
    }
}
