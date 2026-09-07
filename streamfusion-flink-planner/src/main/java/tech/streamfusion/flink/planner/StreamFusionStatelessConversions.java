/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionAggregateShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionExecGraphProcessor.copyEdge;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.util.stream.Collectors;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionStatelessConversions {
    private StreamFusionStatelessConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof StreamExecValues) {
            StreamExecValues values = (StreamExecValues) node;
            return new StreamFusionExecValues(
                    values.getPersistedConfig(),
                    values.getTuples(),
                    (RowType) values.getOutputType(),
                    "StreamFusionValues");
        }
        if (node instanceof BatchExecValues) {
            BatchExecValues values = (BatchExecValues) node;
            return new StreamFusionBatchExecValues(
                    values.getPersistedConfig(),
                    values.getTuples(),
                    (RowType) values.getOutputType(),
                    "StreamFusionBatchValues");
        }
        if (node instanceof StreamExecUnion) {
            StreamExecUnion union = (StreamExecUnion) node;
            StreamFusionExecUnion replacement = new StreamFusionExecUnion(
                    union.getPersistedConfig(),
                    union.getInputProperties(),
                    (RowType) union.getOutputType(),
                    "StreamFusionUnionAll");
            replacement.setInputEdges(union.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecUnion) {
            BatchExecUnion union = (BatchExecUnion) node;
            StreamFusionBatchExecUnion replacement = new StreamFusionBatchExecUnion(
                    union.getPersistedConfig(),
                    union.getInputProperties(),
                    (RowType) union.getOutputType(),
                    "StreamFusionBatchUnionAll");
            replacement.setInputEdges(union.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecExchange) {
            StreamExecExchange exchange = (StreamExecExchange) node;
            StreamFusionExecExchange replacement = new StreamFusionExecExchange(
                    exchange.getPersistedConfig(),
                    exchange.getInputProperties().get(0),
                    (RowType) exchange.getOutputType(),
                    "StreamFusionExchange");
            replacement.setInputEdges(exchange.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecExchange) {
            BatchExecExchange exchange = (BatchExecExchange) node;
            StreamFusionBatchExecExchange replacement = new StreamFusionBatchExecExchange(
                    exchange.getPersistedConfig(),
                    exchange.getInputProperties().get(0),
                    (RowType) exchange.getOutputType(),
                    "StreamFusionBatchExchange");
            replacement.setInputEdges(exchange.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecExpand) {
            StreamExecExpand expand = (StreamExecExpand) node;
            StreamFusionExecExpand replacement = new StreamFusionExecExpand(
                    expand.getPersistedConfig(),
                    projects(expand),
                    expand.getInputProperties().get(0),
                    (RowType) expand.getOutputType(),
                    "StreamFusionExpand");
            replacement.setInputEdges(expand.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecExpand) {
            BatchExecExpand expand = (BatchExecExpand) node;
            StreamFusionBatchExecExpand replacement = new StreamFusionBatchExecExpand(
                    expand.getPersistedConfig(),
                    projects(expand),
                    expand.getInputProperties().get(0),
                    (RowType) expand.getOutputType(),
                    "StreamFusionBatchExpand");
            replacement.setInputEdges(expand.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecWindowTableFunction) {
            StreamExecWindowTableFunction window = (StreamExecWindowTableFunction) node;
            StreamFusionExecWindowTableFunction replacement = new StreamFusionExecWindowTableFunction(
                    window.getPersistedConfig(),
                    windowStrategy(window),
                    window.getInputProperties().get(0),
                    (RowType) window.getOutputType(),
                    "StreamFusionWindowTableFunction");
            replacement.setInputEdges(window.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecWindowTableFunction) {
            BatchExecWindowTableFunction window = (BatchExecWindowTableFunction) node;
            StreamFusionBatchExecWindowTableFunction replacement = new StreamFusionBatchExecWindowTableFunction(
                    window.getPersistedConfig(),
                    windowStrategy(window),
                    window.getInputProperties().get(0),
                    (RowType) window.getOutputType(),
                    "StreamFusionBatchWindowTableFunction");
            replacement.setInputEdges(window.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecCorrelate) {
            StreamExecCorrelate correlate = (StreamExecCorrelate) node;
            org.apache.calcite.rex.RexCall invocation =
                    (org.apache.calcite.rex.RexCall) field(correlate, CommonExecCorrelate.class, "invocation");
            if ("$REPLICATE_ROWS$1".equals(invocation.getOperator().getName())) {
                StreamFusionExecReplicateRows replacement = new StreamFusionExecReplicateRows(
                        correlate.getPersistedConfig(),
                        (org.apache.flink.table.runtime.operators.join.FlinkJoinType)
                                field(correlate, CommonExecCorrelate.class, "joinType"),
                        invocation,
                        correlate.getInputProperties().get(0),
                        (RowType) correlate.getOutputType(),
                        "StreamFusionReplicateRows");
                replacement.setInputEdges(correlate.getInputEdges().stream()
                        .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                        .collect(Collectors.toList()));
                return replacement;
            }
            StreamFusionExecArrayUnnest replacement = new StreamFusionExecArrayUnnest(
                    correlate.getPersistedConfig(),
                    (org.apache.flink.table.runtime.operators.join.FlinkJoinType)
                            field(correlate, CommonExecCorrelate.class, "joinType"),
                    (org.apache.calcite.rex.RexCall) field(correlate, CommonExecCorrelate.class, "invocation"),
                    correlate.getInputProperties().get(0),
                    (RowType) correlate.getOutputType(),
                    "StreamFusionArrayUnnest");
            replacement.setInputEdges(correlate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecCorrelate) {
            BatchExecCorrelate correlate = (BatchExecCorrelate) node;
            StreamFusionBatchExecArrayUnnest replacement = new StreamFusionBatchExecArrayUnnest(
                    correlate.getPersistedConfig(),
                    (org.apache.flink.table.runtime.operators.join.FlinkJoinType)
                            field(correlate, CommonExecCorrelate.class, "joinType"),
                    (org.apache.calcite.rex.RexCall) field(correlate, CommonExecCorrelate.class, "invocation"),
                    correlate.getInputProperties().get(0),
                    (RowType) correlate.getOutputType(),
                    "StreamFusionBatchArrayUnnest");
            replacement.setInputEdges(correlate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
