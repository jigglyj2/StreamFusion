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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionJoinConversions {
    private StreamFusionJoinConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin) {
            var join = (org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin) node;
            var replacement = new StreamFusionExecLookupJoin(
                    join.getPersistedConfig(),
                    join.getInputProperties().get(0),
                    (RowType) join.getOutputType(),
                    StreamFusionLookupJoinSupport.describe(join));
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecHashJoin) {
            BatchExecHashJoin join = (BatchExecHashJoin) node;
            StreamFusionBatchExecHashJoin replacement = new StreamFusionBatchExecHashJoin(
                    join.getPersistedConfig(),
                    batchHashJoinSpec(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionBatchHashJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecAdaptiveJoin) {
            BatchExecAdaptiveJoin join = (BatchExecAdaptiveJoin) node;
            StreamFusionBatchExecHashJoin replacement = new StreamFusionBatchExecHashJoin(
                    join.getPersistedConfig(),
                    batchAdaptiveJoinSpec(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionBatchHashJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecSortMergeJoin) {
            BatchExecSortMergeJoin join = (BatchExecSortMergeJoin) node;
            StreamFusionBatchExecSortMergeJoin replacement = new StreamFusionBatchExecSortMergeJoin(
                    join.getPersistedConfig(),
                    batchSortMergeJoinSpec(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionBatchSortMergeJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecNestedLoopJoin) {
            BatchExecNestedLoopJoin join = (BatchExecNestedLoopJoin) node;
            StreamFusionBatchExecNestedLoopJoin replacement = new StreamFusionBatchExecNestedLoopJoin(
                    join.getPersistedConfig(),
                    batchNestedLoopJoinSpec(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionBatchNestedLoopJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(StreamFusionExecGraphProcessor::bypassBatchExchange)
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecWindowJoin) {
            StreamExecWindowJoin join = (StreamExecWindowJoin) node;
            StreamFusionExecWindowJoin replacement = new StreamFusionExecWindowJoin(
                    join.getPersistedConfig(),
                    windowJoinSpec(join),
                    windowJoinLeftWindowing(join),
                    windowJoinRightWindowing(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionWindowJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecTemporalJoin) {
            StreamExecTemporalJoin join = (StreamExecTemporalJoin) node;
            StreamFusionExecTemporalJoin replacement = new StreamFusionExecTemporalJoin(
                    join.getPersistedConfig(),
                    temporalJoinSpec(join),
                    temporalJoinFunction(join),
                    temporalJoinLeftTimeIndex(join),
                    temporalJoinRightTimeIndex(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionTemporalJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecIntervalJoin) {
            StreamExecIntervalJoin join = (StreamExecIntervalJoin) node;
            StreamFusionExecIntervalJoin replacement = new StreamFusionExecIntervalJoin(
                    join.getPersistedConfig(),
                    intervalJoinSpec(join),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionIntervalJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecMultiJoin) {
            StreamExecMultiJoin join = (StreamExecMultiJoin) node;
            JoinSpec binaryJoin = binaryMultiJoinSpec(join);
            if (binaryJoin != null) {
                List<List<int[]>> uniqueKeys = multiJoinUniqueKeys(join);
                long[] ttl = multiJoinStateTtl(join);
                StreamFusionExecRegularJoin replacement = new StreamFusionExecRegularJoin(
                        join.getPersistedConfig(),
                        binaryJoin,
                        uniqueKeys.get(0),
                        uniqueKeys.get(1),
                        ttl[0],
                        ttl[1],
                        join.getInputProperties().get(0),
                        join.getInputProperties().get(1),
                        (RowType) join.getOutputType(),
                        "StreamFusionRegularJoin[MultiJoin]");
                replacement.setInputEdges(join.getInputEdges().stream()
                        .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                        .collect(Collectors.toList()));
                return replacement;
            }
            StreamFusionExecMultiJoin replacement = new StreamFusionExecMultiJoin(
                    join.getPersistedConfig(),
                    multiJoinTypes(join),
                    multiJoinAttributeMap(join),
                    multiJoinUniqueKeys(join),
                    multiJoinStateTtl(join),
                    multiJoinEquiOnly(join),
                    join.getInputProperties(),
                    (RowType) join.getOutputType(),
                    "StreamFusionMultiJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecJoin) {
            StreamExecJoin join = (StreamExecJoin) node;
            List<Long> ttl = regularJoinStateTtl(join);
            StreamFusionExecRegularJoin replacement = new StreamFusionExecRegularJoin(
                    join.getPersistedConfig(),
                    regularJoinSpec(join),
                    regularJoinLeftUpsertKeys(join),
                    regularJoinRightUpsertKeys(join),
                    ttl.get(0),
                    ttl.get(1),
                    join.getInputProperties().get(0),
                    join.getInputProperties().get(1),
                    (RowType) join.getOutputType(),
                    "StreamFusionRegularJoin");
            replacement.setInputEdges(join.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
