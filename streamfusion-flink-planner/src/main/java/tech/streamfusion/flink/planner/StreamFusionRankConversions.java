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
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.types.logical.RowType;

/** Constructs native physical nodes for this operator family. */
final class StreamFusionRankConversions {
    private StreamFusionRankConversions() {}

    static ExecNode<?> convert(ExecNode<?> node, StreamFusionExecGraphProcessor context) {
        if (node instanceof BatchExecRank) {
            BatchExecRank rank = (BatchExecRank) node;
            BoundedRankPipeline pipeline = boundedRankPipeline(rank);
            if (pipeline != null) {
                return context.convertBoundedRankPipeline(pipeline);
            }
            BatchExecSort inputSort = boundedRankInputSort(rank);
            StreamFusionBatchExecRank replacement = new StreamFusionBatchExecRank(
                    rank.getPersistedConfig(),
                    boundedRankPartitionFields(rank),
                    boundedRankSortFields(rank),
                    boundedRankStart(rank),
                    boundedRankEnd(rank),
                    boundedRankOutputNumber(rank),
                    inputSort == null ? null : boundedSortSpec(inputSort),
                    inputSort == null
                            ? rank.getInputProperties().get(0)
                            : inputSort.getInputProperties().get(0),
                    (RowType) rank.getOutputType(),
                    "StreamFusionBatchRank");
            if (inputSort == null) {
                replacement.setInputEdges(rank.getInputEdges().stream()
                        .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                        .collect(Collectors.toList()));
            } else {
                ExecEdge edge = inputSort.getInputEdges().get(0);
                replacement.setInputEdges(List.of(copyEdge(edge, context.convert(edge.getSource()), replacement)));
            }
            return replacement;
        }
        if (node instanceof BatchExecLimit) {
            BatchExecLimit limit = (BatchExecLimit) node;
            StreamFusionBatchExecLimit replacement = new StreamFusionBatchExecLimit(
                    limit.getPersistedConfig(),
                    boundedLimitStart(limit),
                    boundedLimitEnd(limit),
                    boundedLimitGlobal(limit),
                    limit.getInputProperties().get(0),
                    (RowType) limit.getOutputType(),
                    "StreamFusionBatchLimit[" + (boundedLimitGlobal(limit) ? "global" : "local") + "]");
            replacement.setInputEdges(limit.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecSortLimit) {
            BatchExecSortLimit sort = (BatchExecSortLimit) node;
            StreamFusionBatchExecSortLimit replacement = new StreamFusionBatchExecSortLimit(
                    sort.getPersistedConfig(),
                    boundedSortLimitSpec(sort),
                    boundedSortLimitStart(sort),
                    boundedSortLimitEnd(sort),
                    boundedSortLimitGlobal(sort),
                    sort.getInputProperties().get(0),
                    (RowType) sort.getOutputType(),
                    "StreamFusionBatchSortLimit[" + (boundedSortLimitGlobal(sort) ? "global" : "local") + "]");
            replacement.setInputEdges(sort.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof BatchExecSort) {
            BatchExecSort sort = (BatchExecSort) node;
            StreamFusionBatchExecBoundedSort replacement = new StreamFusionBatchExecBoundedSort(
                    sort.getPersistedConfig(),
                    boundedSortSpec(sort),
                    sort.getInputProperties().get(0),
                    (RowType) sort.getOutputType(),
                    "StreamFusionBatchBoundedSort");
            replacement.setInputEdges(sort.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecDeduplicate) {
            StreamExecDeduplicate deduplicate = (StreamExecDeduplicate) node;
            StreamFusionExecDeduplicate replacement = new StreamFusionExecDeduplicate(
                    deduplicate.getPersistedConfig(),
                    uniqueKeys(deduplicate),
                    booleanField(deduplicate, "isRowtime"),
                    booleanField(deduplicate, "keepLastRow"),
                    booleanField(deduplicate, "outputInsertOnly"),
                    booleanField(deduplicate, "generateUpdateBefore"),
                    stateMetadata(deduplicate),
                    deduplicate.getInputProperties().get(0),
                    (RowType) deduplicate.getOutputType(),
                    "StreamFusionDeduplicate");
            replacement.setInputEdges(deduplicate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecChangelogNormalize) {
            StreamExecChangelogNormalize normalize = (StreamExecChangelogNormalize) node;
            StreamFusionExecChangelogNormalize replacement = new StreamFusionExecChangelogNormalize(
                    normalize.getPersistedConfig(),
                    changelogNormalizeUniqueKeys(normalize),
                    changelogNormalizeGenerateUpdateBefore(normalize),
                    changelogNormalizeFilter(normalize),
                    changelogNormalizeStateMetadata(normalize),
                    normalize.getInputProperties().get(0),
                    (RowType) normalize.getOutputType(),
                    "StreamFusionChangelogNormalize");
            replacement.setInputEdges(normalize.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecWindowDeduplicate) {
            StreamExecWindowDeduplicate deduplicate = (StreamExecWindowDeduplicate) node;
            StreamFusionExecWindowDeduplicate replacement = new StreamFusionExecWindowDeduplicate(
                    deduplicate.getPersistedConfig(),
                    windowDeduplicatePartitionKeys(deduplicate),
                    windowDeduplicateOrderKey(deduplicate),
                    windowDeduplicateKeepLast(deduplicate),
                    windowDeduplicateWindowing(deduplicate),
                    deduplicate.getInputProperties().get(0),
                    (RowType) deduplicate.getOutputType(),
                    "StreamFusionWindowDeduplicate");
            replacement.setInputEdges(deduplicate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecRank) {
            StreamExecRank rank = (StreamExecRank) node;
            RankRange range = rankRange(rank);
            long start = range instanceof ConstantRankRange ? ((ConstantRankRange) range).getRankStart() : 1L;
            Long end = range instanceof ConstantRankRange ? ((ConstantRankRange) range).getRankEnd() : null;
            Integer variableEnd =
                    range instanceof VariableRankRange ? ((VariableRankRange) range).getRankEndIndex() : null;
            StreamFusionExecRank replacement = new StreamFusionExecRank(
                    rank.getPersistedConfig(),
                    rankPartitionKeys(rank),
                    rankSortSpec(rank),
                    rankPrimaryKeys(rank),
                    start,
                    end,
                    variableEnd,
                    rankOutputNumber(rank),
                    rankGenerateUpdateBefore(rank),
                    rankStrategyName(rank),
                    rankStateTtl(rank),
                    rank.getInputProperties().get(0),
                    (RowType) rank.getOutputType(),
                    "StreamFusionRank");
            replacement.setInputEdges(rank.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecWindowRank) {
            StreamExecWindowRank rank = (StreamExecWindowRank) node;
            ConstantRankRange range = (ConstantRankRange) windowRankRange(rank);
            StreamFusionExecWindowRank replacement = new StreamFusionExecWindowRank(
                    rank.getPersistedConfig(),
                    windowRankPartitionKeys(rank),
                    windowRankSortSpec(rank),
                    range.getRankStart(),
                    range.getRankEnd(),
                    windowRankOutputNumber(rank),
                    windowRankWindowing(rank),
                    rank.getInputProperties().get(0),
                    (RowType) rank.getOutputType(),
                    "StreamFusionWindowRank");
            replacement.setInputEdges(rank.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecTemporalSort) {
            StreamExecTemporalSort sort = (StreamExecTemporalSort) node;
            SortSpec sortSpec = temporalSortSpec(sort);
            StreamFusionExecTemporalSort replacement = new StreamFusionExecTemporalSort(
                    sort.getPersistedConfig(),
                    sortSpec,
                    temporalSortProcessingTime(sort, sortSpec),
                    sort.getInputProperties().get(0),
                    (RowType) sort.getOutputType(),
                    "StreamFusionTemporalSort");
            replacement.setInputEdges(sort.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecSort) {
            StreamExecSort sort = (StreamExecSort) node;
            StreamFusionExecBoundedSort replacement = new StreamFusionExecBoundedSort(
                    sort.getPersistedConfig(),
                    boundedSortSpec(sort),
                    sort.getInputProperties().get(0),
                    (RowType) sort.getOutputType(),
                    "StreamFusionBoundedSort");
            replacement.setInputEdges(sort.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, context.convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        return null;
    }
}
