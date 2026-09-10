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
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Verified persistent execution subsets; semantic and backend configuration checks still apply. */
final class StreamFusionPersistentAdmission {
    private static final Set<SqlKind> INTEGER_AGGREGATES =
            Set.of(SqlKind.COUNT, SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX, SqlKind.AVG);

    static String unsupportedReason(ExecNode<?> node, ReadableConfig activeConfig) {
        try {
            if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate)
                return StreamFusionDeduplicateAdmission.unsupportedReason(
                        (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate) node,
                        activeConfig);
            if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank)
                return StreamFusionAppendTopNAdmission.unsupportedReason(
                        (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank) node, activeConfig);
            if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin) {
                var window = (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin) node;
                if (FlinkExecNodeAccess.windowJoinSpec(window).getJoinType() != FlinkJoinType.INNER)
                    return "window join persistent admission: only INNER semantics are verified";
                var config = activeConfig == null ? new Configuration() : Configuration.fromMap(activeConfig.toMap());
                config.addAll(Configuration.fromMap(window.getPersistedConfig().toMap()));
                if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED))
                    return "window join persistent admission: mini-batch control semantics remain unverified";
                return null;
            }
            if (node instanceof StreamExecJoin) {
                var join = FlinkExecNodeAccess.regularJoinSpec((StreamExecJoin) node);
                if (join.getJoinType() != FlinkJoinType.INNER || join.getLeftKeys().length == 0)
                    return "regular join persistent admission: verified shared execution requires an INNER join with equi keys; outer, semi/anti and cross joins remain gated";
                return joinReason(join, ((StreamExecJoin) node).getPersistedConfig(), activeConfig);
            }
            if (node instanceof StreamExecMultiJoin) {
                var join = FlinkExecNodeAccess.binaryMultiJoinSpec((StreamExecMultiJoin) node);
                if (join != null)
                    return joinReason(join, ((StreamExecMultiJoin) node).getPersistedConfig(), activeConfig);
            }
            if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate
                    || node
                            instanceof
                            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate)
                return StreamFusionWindowPersistentAdmission.unsupportedReason(node, activeConfig);
            if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate)
                return StreamFusionSingleStageWindowAdmission.unsupportedReason(
                        (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate) node,
                        activeConfig);
            if (node instanceof StreamExecGroupAggregate)
                return aggregateReason((StreamExecGroupAggregate) node, activeConfig);
        } catch (RuntimeException failure) {
            return "could not verify native persistent execution subset: " + failure.getMessage();
        }
        return "native persistent state is temporarily disabled; large retained-state/buffer admission, "
                + "Flink backend configuration parity, and checkpoint/metric conformance are not yet verified "
                + "for this physical family";
    }

    private static String joinReason(JoinSpec join, ReadableConfig persisted, ReadableConfig active) {
        var config = active == null ? new Configuration() : Configuration.fromMap(active.toMap());
        config.addAll(Configuration.fromMap(persisted.toMap()));
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED))
            return "binary join persistent admission: async-state computation and recovery remain unverified";
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED))
            return "binary join persistent admission: mini-batch bundle and suppression semantics remain unverified";
        if (config.get(org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG))
            return "binary join persistent admission: changelog-state wrapping remains unverified";
        if (join.getNonEquiCondition()
                .map(StreamFusionPersistentAdmission::boundedPredicate)
                .orElse(true)) return null;
        return "binary join residual workspace: only boolean combinations of direct column/literal "
                + "comparisons and null checks, including TIMESTAMP(3) column +/- literal day-time intervals, "
                + "have verified bounded Arrow workspace; other computed predicate operands remain on Flink";
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
            if (StreamFusionStringAggregateAdmission.isStringExtremum(call)) {
                String reason = StreamFusionStringAggregateAdmission.unsupportedReason(aggregate, call, input);
                if (reason != null) return reason;
                continue;
            }
            if (!INTEGER_AGGREGATES.contains(call.getAggregation().getKind())
                    || (call.isDistinct() && call.getAggregation().getKind() != SqlKind.COUNT)
                    || call.getType().getSqlTypeName() != org.apache.calcite.sql.type.SqlTypeName.BIGINT)
                return "aggregate persistent admission: verified calls are BIGINT COUNT (including DISTINCT) "
                        + "and non-DISTINCT BIGINT SUM/SUM0/MIN/MAX/AVG";
            for (int index : call.getArgList())
                if (input.getTypeAt(index).getTypeRoot() != LogicalTypeRoot.BIGINT)
                    return "aggregate persistent admission: aggregate argument type " + input.getTypeAt(index)
                            + " lacks the complete metric/recovery proof for production";
        }
        return null;
    }

    private static boolean boundedOperand(RexNode operand) {
        if (operand.getKind() == SqlKind.INPUT_REF || operand.getKind() == SqlKind.LITERAL) return true;
        if (!(operand instanceof RexCall)) return false;
        var call = (RexCall) operand;
        if ((call.getKind() != SqlKind.PLUS && call.getKind() != SqlKind.MINUS)
                || call.getOperands().size() != 2) return false;
        var timestamp = call.getOperands().get(0);
        var interval = call.getOperands().get(1);
        // One fixed-width arithmetic result. Dynamic intervals, nested arithmetic, other
        // precisions and calendar/time-zone operations retain their semantic/workspace gate.
        return timestamp.getKind() == SqlKind.INPUT_REF
                && timestamp.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
                && timestamp.getType().getPrecision() == 3
                && call.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
                && call.getType().getPrecision() == 3
                && interval.getKind() == SqlKind.LITERAL
                && !org.apache.calcite.rex.RexLiteral.isNullLiteral(interval)
                && interval.getType().getSqlTypeName().getFamily()
                        == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME;
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
                return call.getOperands().stream().allMatch(StreamFusionPersistentAdmission::boundedOperand);
            default:
                return false;
        }
    }
}
