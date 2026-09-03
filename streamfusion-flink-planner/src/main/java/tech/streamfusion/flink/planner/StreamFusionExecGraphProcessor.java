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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TimeUtils;

/** All-or-nothing physical rule modelled after Comet's distinct accelerator exec nodes. */
public final class StreamFusionExecGraphProcessor implements ExecNodeGraphProcessor {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.calc.StreamFusionCalcTranslator";
    private static final String UNNEST_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.unnest.StreamFusionArrayUnnestTranslator";
    private static final String EXPAND_TRANSLATOR_CLASS = "tech.streamfusion.flink.expand.StreamFusionExpandTranslator";
    private static final String VALUES_TRANSLATOR_CLASS = "tech.streamfusion.flink.values.StreamFusionValuesTranslator";
    private static final String UNION_TRANSLATOR_CLASS = "tech.streamfusion.flink.union.StreamFusionUnionTranslator";
    private static final String WINDOW_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowTableFunctionTranslator";
    private static final String DEDUPLICATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateTranslator";
    private static final String CHANGELOG_NORMALIZE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.changelog.StreamFusionChangelogNormalizeTranslator";
    private static final String GROUP_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator";
    private static final String WINDOW_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowAggregateTranslator";
    private static final String WINDOW_DEDUPLICATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowDeduplicateTranslator";
    private static final String WINDOW_RANK_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowRankTranslator";
    private static final String TOP_N_TRANSLATOR_CLASS = "tech.streamfusion.flink.topn.StreamFusionTopNTranslator";
    private static final String WINDOW_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowJoinTranslator";
    private static final String REGULAR_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionRegularJoinTranslator";
    private static final String INTERVAL_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionIntervalJoinTranslator";
    private static final String OVER_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.over.StreamFusionOverAggregateTranslator";

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        StreamFusionPlanningDiagnostics.begin();
        List<String> rejections = new ArrayList<>();
        for (int index = 0; index < graph.getRootNodes().size(); index++) {
            collectRejections(graph.getRootNodes().get(index), context, "root[" + index + "]", rejections);
        }
        if (!rejections.isEmpty()) {
            rejections.forEach(rejection -> {
                int separator = rejection.indexOf('\n');
                StreamFusionPlanningDiagnostics.reject(
                        rejection.substring(0, separator), rejection.substring(separator + 1));
            });
            return graph;
        }
        StreamFusionPlanningDiagnostics.accelerate();
        List<ExecNode<?>> roots =
                graph.getRootNodes().stream().map(this::convertRoot).collect(Collectors.toList());
        return new ExecNodeGraph(graph.getFlinkVersion(), roots);
    }

    private ExecNode<?> convertRoot(ExecNode<?> root) {
        if (isSinkBoundary(root)) {
            return convert(root);
        }
        ExecNode<?> converted = convert(root);
        StreamFusionExecSinkBoundary boundary = new StreamFusionExecSinkBoundary(
                ((ExecNodeBase<?>) root).getPersistedConfig(), InputProperty.DEFAULT, (RowType) root.getOutputType());
        boundary.setInputEdges(List.of(ExecEdge.builder()
                .source(converted)
                .target(boundary)
                .shuffle(ExecEdge.FORWARD_SHUFFLE)
                .build()));
        return boundary;
    }

    private void collectRejections(ExecNode<?> node, ProcessorContext context, String path, List<String> rejections) {
        String nodePath = path + "/" + node.getClass().getSimpleName();
        if (node instanceof StreamExecValues) {
            String reason = unsupportedReason((StreamExecValues) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node.getInputEdges().isEmpty()) {
            return;
        } else if (node instanceof StreamExecCalc) {
            String reason = unsupportedReason((StreamExecCalc) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecCorrelate) {
            String reason = unsupportedReason((StreamExecCorrelate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecChangelogNormalize) {
            String reason = unsupportedReason((StreamExecChangelogNormalize) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecDeduplicate) {
            String reason = unsupportedReason((StreamExecDeduplicate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecGroupAggregate) {
            String reason = unsupportedReason((StreamExecGroupAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecOverAggregate) {
            String reason = unsupportedReason((StreamExecOverAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecGlobalWindowAggregate) {
            TwoPhaseWindowAggregate twoPhase = twoPhaseWindowAggregate((StreamExecGlobalWindowAggregate) node);
            if (twoPhase == null) {
                rejections.add(nodePath
                        + "\nglobal window aggregate: expected LocalWindowAggregate -> Exchange -> "
                        + "GlobalWindowAggregate so the native operator can consume original rows");
                return;
            }
            String reason = unsupportedReason(twoPhase, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(twoPhase.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof StreamExecLocalWindowAggregate) {
            rejections.add(
                    nodePath + "\nlocal window aggregate: native acceleration requires its paired global aggregate");
        } else if (node instanceof StreamExecWindowAggregate) {
            ProcessingTimeWindowAggregate folded = processingTimeWindowAggregate((StreamExecWindowAggregate) node);
            String reason = folded == null
                    ? unsupportedReason((StreamExecWindowAggregate) node, context)
                    : unsupportedReason(folded, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (folded != null) {
                collectRejections(folded.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof StreamExecWindowDeduplicate) {
            String reason = unsupportedReason((StreamExecWindowDeduplicate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecRank) {
            String reason = unsupportedReason((StreamExecRank) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowRank) {
            String reason = unsupportedReason((StreamExecWindowRank) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowJoin) {
            String reason = unsupportedReason((StreamExecWindowJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecIntervalJoin) {
            String reason = unsupportedReason((StreamExecIntervalJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecJoin) {
            String reason = unsupportedReason((StreamExecJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecDropUpdateBefore) {
            // RowKind is Flink changelog metadata, so this node is always eligible.
        } else if (node instanceof StreamExecMiniBatchAssigner) {
            // Native stateful operators already consume Arrow mini-batches. The latency-marker
            // assigner is folded into the native stateful node during conversion.
        } else if (node instanceof StreamExecWatermarkAssigner) {
            // The distinct node retains Flink's generated expression and watermark state machine.
        } else if (node instanceof StreamExecExpand) {
            String reason = unsupportedReason((StreamExecExpand) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecExchange) {
            StreamExecExchange exchange = (StreamExecExchange) node;
            String reason = StreamFusionExchangeSupport.unsupportedReason(
                    (RowType) exchange.getOutputType(),
                    exchange.getInputProperties().get(0).getRequiredDistribution());
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecUnion) {
            String reason = unsupportedReason((StreamExecUnion) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowTableFunction) {
            String reason = unsupportedReason((StreamExecWindowTableFunction) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (!(node instanceof StreamExecUnion) && !isSinkBoundary(node)) {
            rejections.add(nodePath + "\noperator has no StreamFusion physical implementation");
        }
        for (int index = 0; index < node.getInputEdges().size(); index++) {
            collectRejections(
                    node.getInputEdges().get(index).getSource(),
                    context,
                    nodePath + "/input[" + index + "]",
                    rejections);
        }
    }

    private static boolean isSinkBoundary(ExecNode<?> node) {
        String nodeName = node.getClass().getSimpleName();
        return nodeName.equals("StreamExecSink") || nodeName.equals("StreamExecLegacySink");
    }

    ExecNode<?> convert(ExecNode<?> node) {
        if (isSinkBoundary(node)) {
            for (int index = 0; index < node.getInputEdges().size(); index++) {
                ExecEdge edge = node.getInputEdges().get(index);
                ExecNode<?> convertedSource = convert(edge.getSource());
                StreamFusionExecSinkBoundary boundary = new StreamFusionExecSinkBoundary(
                        ((ExecNodeBase<?>) node).getPersistedConfig(),
                        node.getInputProperties().get(index),
                        (RowType) edge.getOutputType());
                boundary.setInputEdges(List.of(copyEdge(edge, convertedSource, boundary)));
                node.replaceInputEdge(index, copyEdge(edge, boundary, node));
            }
            return node;
        }
        if (node instanceof StreamExecValues) {
            StreamExecValues values = (StreamExecValues) node;
            return new StreamFusionExecValues(
                    values.getPersistedConfig(),
                    values.getTuples(),
                    (RowType) values.getOutputType(),
                    "StreamFusionValues");
        }
        if (node instanceof StreamExecCalc) {
            StreamExecCalc calc = (StreamExecCalc) node;
            StreamFusionExecCalc replacement = new StreamFusionExecCalc(
                    calc.getPersistedConfig(),
                    projection(calc),
                    condition(calc),
                    calc.getInputProperties().get(0),
                    (RowType) calc.getOutputType(),
                    "StreamFusionCalc");
            replacement.setInputEdges(calc.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecGroupAggregate) {
            StreamExecGroupAggregate aggregate = (StreamExecGroupAggregate) node;
            StreamFusionExecGroupAggregate replacement = new StreamFusionExecGroupAggregate(
                    aggregate.getPersistedConfig(),
                    grouping(aggregate),
                    aggregateCalls(aggregate),
                    aggregateCallNeedRetractions(aggregate),
                    aggregateBooleanField(aggregate, "generateUpdateBefore"),
                    aggregateBooleanField(aggregate, "needRetraction"),
                    aggregateStateMetadata(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionGroupAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecOverAggregate) {
            StreamExecOverAggregate aggregate = (StreamExecOverAggregate) node;
            StreamFusionExecOverAggregate replacement = new StreamFusionExecOverAggregate(
                    aggregate.getPersistedConfig(),
                    overSpec(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionOverAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecGlobalWindowAggregate) {
            TwoPhaseWindowAggregate twoPhase = twoPhaseWindowAggregate((StreamExecGlobalWindowAggregate) node);
            if (twoPhase == null) {
                throw new IllegalStateException("Selected malformed two-phase window aggregate");
            }
            StreamExecGlobalWindowAggregate global = twoPhase.global;
            StreamExecLocalWindowAggregate local = twoPhase.local;
            StreamFusionExecWindowAggregate replacement = new StreamFusionExecWindowAggregate(
                    global.getPersistedConfig(),
                    localWindowGrouping(local),
                    localWindowAggregateCalls(local),
                    localWindowing(local),
                    globalWindowProperties(global),
                    globalWindowNeedRetraction(global),
                    local.getInputProperties().get(0),
                    (RowType) global.getOutputType(),
                    "StreamFusionWindowAggregate");
            replacement.setInputEdges(
                    List.of(copyEdge(twoPhase.inputEdge, convert(twoPhase.inputEdge.getSource()), replacement)));
            return replacement;
        }
        if (node instanceof StreamExecWindowAggregate) {
            StreamExecWindowAggregate aggregate = (StreamExecWindowAggregate) node;
            ProcessingTimeWindowAggregate folded = processingTimeWindowAggregate(aggregate);
            if (folded != null) {
                StreamFusionExecWindowAggregate replacement = new StreamFusionExecWindowAggregate(
                        aggregate.getPersistedConfig(),
                        folded.grouping,
                        folded.aggregateCalls,
                        folded.windowing,
                        windowProperties(aggregate),
                        windowNeedRetraction(aggregate),
                        folded.inputProperty,
                        (RowType) aggregate.getOutputType(),
                        "StreamFusionWindowAggregate");
                replacement.setInputEdges(
                        List.of(copyEdge(folded.inputEdge, convert(folded.inputEdge.getSource()), replacement)));
                return replacement;
            }
            StreamFusionExecWindowAggregate replacement = new StreamFusionExecWindowAggregate(
                    aggregate.getPersistedConfig(),
                    windowGrouping(aggregate),
                    windowAggregateCalls(aggregate),
                    windowing(aggregate),
                    windowProperties(aggregate),
                    windowNeedRetraction(aggregate),
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionWindowAggregate");
            replacement.setInputEdges(aggregate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecDropUpdateBefore) {
            StreamExecDropUpdateBefore drop = (StreamExecDropUpdateBefore) node;
            StreamFusionExecDropUpdateBefore replacement = new StreamFusionExecDropUpdateBefore(
                    drop.getPersistedConfig(),
                    drop.getInputProperties().get(0),
                    (RowType) drop.getOutputType(),
                    "StreamFusionDropUpdateBefore");
            replacement.setInputEdges(drop.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecMiniBatchAssigner) {
            ExecEdge edge = node.getInputEdges().get(0);
            return convert(edge.getSource());
        }
        if (node instanceof StreamExecWatermarkAssigner) {
            StreamExecWatermarkAssigner watermark = (StreamExecWatermarkAssigner) node;
            StreamFusionExecWatermarkAssigner replacement = new StreamFusionExecWatermarkAssigner(
                    watermark.getPersistedConfig(),
                    watermarkExpression(watermark),
                    watermarkRowtimeFieldIndex(watermark),
                    watermark.getInputProperties().get(0),
                    (RowType) watermark.getOutputType(),
                    "StreamFusionWatermarkAssigner");
            replacement.setInputEdges(watermark.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecUnion) {
            StreamExecUnion union = (StreamExecUnion) node;
            StreamFusionExecUnion replacement = new StreamFusionExecUnion(
                    union.getPersistedConfig(),
                    union.getInputProperties(),
                    (RowType) union.getOutputType(),
                    "StreamFusionUnionAll");
            replacement.setInputEdges(union.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        if (node instanceof StreamExecCorrelate) {
            StreamExecCorrelate correlate = (StreamExecCorrelate) node;
            StreamFusionExecArrayUnnest replacement = new StreamFusionExecArrayUnnest(
                    correlate.getPersistedConfig(),
                    (org.apache.flink.table.runtime.operators.join.FlinkJoinType)
                            field(correlate, CommonExecCorrelate.class, "joinType"),
                    (org.apache.calcite.rex.RexCall) field(correlate, CommonExecCorrelate.class, "invocation"),
                    correlate.getInputProperties().get(0),
                    (RowType) correlate.getOutputType(),
                    "StreamFusionArrayUnnest");
            replacement.setInputEdges(correlate.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
        }
        for (int index = 0; index < node.getInputEdges().size(); index++) {
            ExecEdge edge = node.getInputEdges().get(index);
            node.replaceInputEdge(index, copyEdge(edge, convert(edge.getSource()), node));
        }
        return node;
    }

    private static ExecEdge copyEdge(ExecEdge edge, ExecNode<?> source, ExecNode<?> target) {
        return ExecEdge.builder()
                .source(source)
                .target(target)
                .shuffle(edge.getShuffle())
                .exchangeMode(edge.getExchangeMode())
                .build();
    }

    private String unsupportedReason(StreamExecCalc calc, ProcessorContext context) {
        ExecEdge input = calc.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method =
                    translator.getMethod("unsupportedReason", RowType.class, RowType.class, List.class, Object.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) calc.getOutputType(),
                    projection(calc),
                    condition(calc));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion calc support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion calc support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecDeduplicate deduplicate, ProcessorContext context) {
        ExecEdge input = deduplicate.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    DEDUPLICATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) deduplicate.getOutputType(),
                    uniqueKeys(deduplicate),
                    booleanField(deduplicate, "isRowtime"),
                    booleanField(deduplicate, "keepLastRow"),
                    booleanField(deduplicate, "outputInsertOnly"),
                    booleanField(deduplicate, "generateUpdateBefore"),
                    stateTtl(deduplicate),
                    deduplicate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Deduplicate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Deduplicate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecChangelogNormalize normalize, ProcessorContext context) {
        ExecEdge input = normalize.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    CHANGELOG_NORMALIZE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, RowType.class, int[].class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) normalize.getOutputType(),
                    changelogNormalizeUniqueKeys(normalize),
                    normalize.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion ChangelogNormalize support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion ChangelogNormalize support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecWindowDeduplicate deduplicate, ProcessorContext context) {
        ExecEdge input = deduplicate.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_DEDUPLICATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    int.class,
                    WindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) deduplicate.getOutputType(),
                    windowDeduplicatePartitionKeys(deduplicate),
                    windowDeduplicateOrderKey(deduplicate),
                    windowDeduplicateWindowing(deduplicate),
                    deduplicate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowDeduplicate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowDeduplicate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecRank rank, ProcessorContext context) {
        if (rankType(rank) != RankType.ROW_NUMBER) {
            return "rank type: Flink streaming Top-N only implements ROW_NUMBER";
        }
        RankRange range = rankRange(rank);
        long start;
        Long end;
        Integer variableEnd;
        if (range instanceof ConstantRankRange) {
            start = ((ConstantRankRange) range).getRankStart();
            end = ((ConstantRankRange) range).getRankEnd();
            variableEnd = null;
        } else if (range instanceof VariableRankRange) {
            start = 1L;
            end = null;
            variableEnd = ((VariableRankRange) range).getRankEndIndex();
        } else {
            return "rank range: Flink Top-N requires a constant or variable rank end";
        }
        ExecEdge input = rank.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    TOP_N_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    SortSpec.class,
                    int[].class,
                    long.class,
                    Long.class,
                    Integer.class,
                    boolean.class,
                    String.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) rank.getOutputType(),
                    rankPartitionKeys(rank),
                    rankSortSpec(rank),
                    rankPrimaryKeys(rank),
                    start,
                    end,
                    variableEnd,
                    rankOutputNumber(rank),
                    rankStrategyName(rank),
                    rankStateTtl(rank),
                    rank.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Top-N support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Top-N support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecWindowRank rank, ProcessorContext context) {
        if (windowRankType(rank) != RankType.ROW_NUMBER) {
            return "rank type: Flink Window Top-N only implements ROW_NUMBER";
        }
        RankRange range = windowRankRange(rank);
        if (!(range instanceof ConstantRankRange)) {
            return "rank range: Window Top-N requires a constant range";
        }
        ConstantRankRange constant = (ConstantRankRange) range;
        ExecEdge input = rank.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_RANK_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    SortSpec.class,
                    long.class,
                    long.class,
                    boolean.class,
                    WindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) rank.getOutputType(),
                    windowRankPartitionKeys(rank),
                    windowRankSortSpec(rank),
                    constant.getRankStart(),
                    constant.getRankEnd(),
                    windowRankOutputNumber(rank),
                    windowRankWindowing(rank),
                    rank.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowRank support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowRank support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecWindowJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    WindowingStrategy.class,
                    WindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    windowJoinSpec(join),
                    windowJoinLeftWindowing(join),
                    windowJoinRightWindowing(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowJoin support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowJoin support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        List<Long> ttl = regularJoinStateTtl(join);
        try {
            Class<?> translator = Class.forName(
                    REGULAR_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    List.class,
                    List.class,
                    long.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    regularJoinSpec(join),
                    regularJoinLeftUpsertKeys(join),
                    regularJoinRightUpsertKeys(join),
                    ttl.get(0),
                    ttl.get(1),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion regular join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion regular join support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecIntervalJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    INTERVAL_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    IntervalJoinSpec.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    intervalJoinSpec(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion interval join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion interval join support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecGroupAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        ExecNode<?> inputSource = input.getSource();
        if (inputSource instanceof StreamExecExchange
                && inputSource.getInputEdges().size() == 1) {
            inputSource = inputSource.getInputEdges().get(0).getSource();
        }
        if (inputSource instanceof StreamExecExpand) {
            return "grouping sets: native group aggregate does not yet implement expanded grouping-set semantics";
        }
        try {
            Class<?> translator = Class.forName(
                    GROUP_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    boolean[].class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    grouping(aggregate),
                    aggregateCalls(aggregate),
                    aggregateCallNeedRetractions(aggregate),
                    aggregateBooleanField(aggregate, "generateUpdateBefore"),
                    aggregateBooleanField(aggregate, "needRetraction"),
                    aggregateStateTtl(aggregate),
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion GroupAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion GroupAggregate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecOverAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        long stateTtl = aggregate
                .getPersistedConfig()
                .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                .toMillis();
        try {
            Class<?> translator = Class.forName(
                    OVER_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    OverSpec.class,
                    long.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    overSpec(aggregate),
                    stateTtl,
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion OVER support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion OVER support inspection failed", failure.getCause());
        }
    }

    private String unsupportedReason(StreamExecWindowAggregate aggregate, ProcessorContext context) {
        ExecEdge input = aggregate.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) aggregate.getOutputType(),
                    windowGrouping(aggregate),
                    windowAggregateCalls(aggregate),
                    windowing(aggregate),
                    windowProperties(aggregate),
                    windowNeedRetraction(aggregate),
                    aggregate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion WindowAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowAggregate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(ProcessingTimeWindowAggregate aggregate, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) aggregate.inputEdge.getOutputType(),
                    (RowType) aggregate.node.getOutputType(),
                    aggregate.grouping,
                    aggregate.aggregateCalls,
                    aggregate.windowing,
                    windowProperties(aggregate.node),
                    windowNeedRetraction(aggregate.node),
                    aggregate.node.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect folded processing-time WindowAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(
                    "Folded processing-time WindowAggregate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(TwoPhaseWindowAggregate aggregate, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) aggregate.inputEdge.getOutputType(),
                    (RowType) aggregate.global.getOutputType(),
                    localWindowGrouping(aggregate.local),
                    localWindowAggregateCalls(aggregate.local),
                    localWindowing(aggregate.local),
                    globalWindowProperties(aggregate.global),
                    globalWindowNeedRetraction(aggregate.global),
                    aggregate.global.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion two-phase WindowAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(
                    "StreamFusion two-phase WindowAggregate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecCorrelate correlate, ProcessorContext context) {
        ExecEdge input = correlate.getInputEdges().get(0);
        Object joinType = field(correlate, CommonExecCorrelate.class, "joinType");
        Object invocation = field(correlate, CommonExecCorrelate.class, "invocation");
        Object condition = field(correlate, CommonExecCorrelate.class, "condition");
        try {
            Class<?> translator = Class.forName(
                    UNNEST_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, RowType.class, Object.class, Object.class, Object.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) correlate.getOutputType(),
                    joinType,
                    invocation,
                    condition);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion array UNNEST support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion array UNNEST support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecExpand expand, ProcessorContext context) {
        ExecEdge input = expand.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    EXPAND_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class, RowType.class, List.class);
            return (String) method.invoke(
                    null, (RowType) input.getOutputType(), (RowType) expand.getOutputType(), projects(expand));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Expand support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Expand support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecValues values, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    VALUES_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class, List.class);
            return (String) method.invoke(null, (RowType) values.getOutputType(), values.getTuples());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion VALUES support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion VALUES support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecUnion union, ProcessorContext context) {
        if (context == null) {
            return null;
        }
        try {
            Class<?> translator = Class.forName(
                    UNION_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod("unsupportedReason", RowType.class);
            return (String) method.invoke(null, (RowType) union.getOutputType());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion UNION ALL support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion UNION ALL support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecWindowTableFunction window, ProcessorContext context) {
        ExecEdge input = window.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    WINDOW_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    TimeAttributeWindowingStrategy.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) window.getOutputType(),
                    windowStrategy(window),
                    window.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion Window TVF support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Window TVF support inspection failed", e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    private static List<RexNode> projection(StreamExecCalc calc) {
        return (List<RexNode>) field(calc, CommonExecCalc.class, "projection");
    }

    private static RexNode condition(StreamExecCalc calc) {
        return (RexNode) field(calc, CommonExecCalc.class, "condition");
    }

    @SuppressWarnings("unchecked")
    private static List<List<RexNode>> projects(StreamExecExpand expand) {
        return (List<List<RexNode>>) field(expand, CommonExecExpand.class, "projects");
    }

    private static TimeAttributeWindowingStrategy windowStrategy(StreamExecWindowTableFunction window) {
        return (TimeAttributeWindowingStrategy) field(window, CommonExecWindowTableFunction.class, "windowingStrategy");
    }

    private static RexNode watermarkExpression(StreamExecWatermarkAssigner watermark) {
        return (RexNode) field(watermark, StreamExecWatermarkAssigner.class, "watermarkExpr");
    }

    private static int watermarkRowtimeFieldIndex(StreamExecWatermarkAssigner watermark) {
        return (int) field(watermark, StreamExecWatermarkAssigner.class, "rowtimeFieldIndex");
    }

    private static int[] uniqueKeys(StreamExecDeduplicate deduplicate) {
        return ((int[]) field(deduplicate, StreamExecDeduplicate.class, "uniqueKeys")).clone();
    }

    private static boolean booleanField(StreamExecDeduplicate deduplicate, String name) {
        return (boolean) field(deduplicate, StreamExecDeduplicate.class, name);
    }

    @SuppressWarnings("unchecked")
    private static List<StateMetadata> stateMetadata(StreamExecDeduplicate deduplicate) {
        return (List<StateMetadata>) field(deduplicate, StreamExecDeduplicate.class, "stateMetadataList");
    }

    private static long stateTtl(StreamExecDeduplicate deduplicate) {
        List<StateMetadata> metadata = stateMetadata(deduplicate);
        if (metadata == null || metadata.isEmpty()) {
            return deduplicate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    private static int[] changelogNormalizeUniqueKeys(StreamExecChangelogNormalize normalize) {
        return ((int[]) field(normalize, StreamExecChangelogNormalize.class, "uniqueKeys")).clone();
    }

    private static boolean changelogNormalizeGenerateUpdateBefore(StreamExecChangelogNormalize normalize) {
        return (boolean) field(normalize, StreamExecChangelogNormalize.class, "generateUpdateBefore");
    }

    private static RexNode changelogNormalizeFilter(StreamExecChangelogNormalize normalize) {
        return (RexNode) field(normalize, StreamExecChangelogNormalize.class, "filterCondition");
    }

    @SuppressWarnings("unchecked")
    private static List<StateMetadata> changelogNormalizeStateMetadata(StreamExecChangelogNormalize normalize) {
        return (List<StateMetadata>) field(normalize, StreamExecChangelogNormalize.class, "stateMetadataList");
    }

    private static int[] grouping(StreamExecGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecGroupAggregate.class, "grouping")).clone();
    }

    private static OverSpec overSpec(StreamExecOverAggregate aggregate) {
        return (OverSpec) field(aggregate, StreamExecOverAggregate.class, "overSpec");
    }

    private static org.apache.calcite.rel.core.AggregateCall[] aggregateCalls(StreamExecGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecGroupAggregate.class, "aggCalls"))
                .clone();
    }

    private static boolean[] aggregateCallNeedRetractions(StreamExecGroupAggregate aggregate) {
        return ((boolean[]) field(aggregate, StreamExecGroupAggregate.class, "aggCallNeedRetractions")).clone();
    }

    private static boolean aggregateBooleanField(StreamExecGroupAggregate aggregate, String name) {
        return (boolean) field(aggregate, StreamExecGroupAggregate.class, name);
    }

    @SuppressWarnings("unchecked")
    private static List<StateMetadata> aggregateStateMetadata(StreamExecGroupAggregate aggregate) {
        return (List<StateMetadata>) field(aggregate, StreamExecGroupAggregate.class, "stateMetadataList");
    }

    private static long aggregateStateTtl(StreamExecGroupAggregate aggregate) {
        List<StateMetadata> metadata = aggregateStateMetadata(aggregate);
        if (metadata == null || metadata.isEmpty()) {
            return aggregate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    private static int[] windowGrouping(StreamExecWindowAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecWindowAggregate.class, "grouping")).clone();
    }

    private static int[] windowDeduplicatePartitionKeys(StreamExecWindowDeduplicate deduplicate) {
        return ((int[]) field(deduplicate, StreamExecWindowDeduplicate.class, "partitionKeys")).clone();
    }

    private static int windowDeduplicateOrderKey(StreamExecWindowDeduplicate deduplicate) {
        return (int) field(deduplicate, StreamExecWindowDeduplicate.class, "orderKey");
    }

    private static boolean windowDeduplicateKeepLast(StreamExecWindowDeduplicate deduplicate) {
        return (boolean) field(deduplicate, StreamExecWindowDeduplicate.class, "keepLastRow");
    }

    private static WindowingStrategy windowDeduplicateWindowing(StreamExecWindowDeduplicate deduplicate) {
        return (WindowingStrategy) field(deduplicate, StreamExecWindowDeduplicate.class, "windowing");
    }

    private static RankType rankType(StreamExecRank rank) {
        return (RankType) field(rank, StreamExecRank.class, "rankType");
    }

    private static int[] rankPartitionKeys(StreamExecRank rank) {
        return ((PartitionSpec) field(rank, StreamExecRank.class, "partitionSpec")).getFieldIndices();
    }

    private static SortSpec rankSortSpec(StreamExecRank rank) {
        return (SortSpec) field(rank, StreamExecRank.class, "sortSpec");
    }

    private static RankRange rankRange(StreamExecRank rank) {
        return (RankRange) field(rank, StreamExecRank.class, "rankRange");
    }

    private static RankProcessStrategy rankStrategy(StreamExecRank rank) {
        return (RankProcessStrategy) field(rank, StreamExecRank.class, "rankStrategy");
    }

    private static String rankStrategyName(StreamExecRank rank) {
        RankProcessStrategy strategy = rankStrategy(rank);
        if (strategy instanceof RankProcessStrategy.AppendFastStrategy) {
            return "APPEND_FAST";
        }
        if (strategy instanceof RankProcessStrategy.UpdateFastStrategy) {
            return "UPDATE_FAST";
        }
        if (strategy instanceof RankProcessStrategy.RetractStrategy) {
            return "RETRACT";
        }
        return null;
    }

    private static int[] rankPrimaryKeys(StreamExecRank rank) {
        RankProcessStrategy strategy = rankStrategy(rank);
        return strategy instanceof RankProcessStrategy.UpdateFastStrategy
                ? ((RankProcessStrategy.UpdateFastStrategy) strategy)
                        .getPrimaryKeys()
                        .clone()
                : new int[0];
    }

    private static boolean rankOutputNumber(StreamExecRank rank) {
        return (boolean) field(rank, StreamExecRank.class, "outputRankNumber");
    }

    private static boolean rankGenerateUpdateBefore(StreamExecRank rank) {
        return (boolean) field(rank, StreamExecRank.class, "generateUpdateBefore");
    }

    @SuppressWarnings("unchecked")
    private static long rankStateTtl(StreamExecRank rank) {
        List<StateMetadata> metadata = (List<StateMetadata>) field(rank, StreamExecRank.class, "stateMetadataList");
        if (metadata == null || metadata.isEmpty()) {
            return rank.getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    private static RankType windowRankType(StreamExecWindowRank rank) {
        return (RankType) field(rank, StreamExecWindowRank.class, "rankType");
    }

    private static int[] windowRankPartitionKeys(StreamExecWindowRank rank) {
        return ((PartitionSpec) field(rank, StreamExecWindowRank.class, "partitionSpec")).getFieldIndices();
    }

    private static SortSpec windowRankSortSpec(StreamExecWindowRank rank) {
        return (SortSpec) field(rank, StreamExecWindowRank.class, "sortSpec");
    }

    private static RankRange windowRankRange(StreamExecWindowRank rank) {
        return (RankRange) field(rank, StreamExecWindowRank.class, "rankRange");
    }

    private static boolean windowRankOutputNumber(StreamExecWindowRank rank) {
        return (boolean) field(rank, StreamExecWindowRank.class, "outputRankNumber");
    }

    private static WindowingStrategy windowRankWindowing(StreamExecWindowRank rank) {
        return (WindowingStrategy) field(rank, StreamExecWindowRank.class, "windowing");
    }

    private static JoinSpec windowJoinSpec(StreamExecWindowJoin join) {
        return (JoinSpec) field(join, StreamExecWindowJoin.class, "joinSpec");
    }

    private static JoinSpec regularJoinSpec(StreamExecJoin join) {
        return (JoinSpec) field(join, StreamExecJoin.class, "joinSpec");
    }

    private static IntervalJoinSpec intervalJoinSpec(StreamExecIntervalJoin join) {
        return (IntervalJoinSpec) field(join, StreamExecIntervalJoin.class, "intervalJoinSpec");
    }

    @SuppressWarnings("unchecked")
    private static List<int[]> regularJoinLeftUpsertKeys(StreamExecJoin join) {
        return (List<int[]>) field(join, StreamExecJoin.class, "leftUpsertKeys");
    }

    @SuppressWarnings("unchecked")
    private static List<int[]> regularJoinRightUpsertKeys(StreamExecJoin join) {
        return (List<int[]>) field(join, StreamExecJoin.class, "rightUpsertKeys");
    }

    @SuppressWarnings("unchecked")
    private static List<Long> regularJoinStateTtl(StreamExecJoin join) {
        List<StateMetadata> metadata = (List<StateMetadata>) field(join, StreamExecJoin.class, "stateMetadataList");
        if (metadata == null || metadata.isEmpty()) {
            long fallback = join.getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
            return List.of(fallback, fallback);
        }
        if (metadata.size() != 2) {
            throw new IllegalStateException("Regular join must define exactly two state TTL entries");
        }
        long[] ttl = new long[2];
        boolean[] seen = new boolean[2];
        for (StateMetadata state : metadata) {
            int index = state.getStateIndex();
            if (index < 0 || index >= 2 || seen[index]) {
                throw new IllegalStateException("Regular join state TTL indices must be unique 0 and 1");
            }
            ttl[index] = TimeUtils.parseDuration(state.getStateTtl()).toMillis();
            seen[index] = true;
        }
        if (!seen[0] || !seen[1]) {
            throw new IllegalStateException("Regular join state TTL indices must contain 0 and 1");
        }
        return List.of(ttl[0], ttl[1]);
    }

    private static WindowingStrategy windowJoinLeftWindowing(StreamExecWindowJoin join) {
        return (WindowingStrategy) field(join, StreamExecWindowJoin.class, "leftWindowing");
    }

    private static WindowingStrategy windowJoinRightWindowing(StreamExecWindowJoin join) {
        return (WindowingStrategy) field(join, StreamExecWindowJoin.class, "rightWindowing");
    }

    private static org.apache.calcite.rel.core.AggregateCall[] windowAggregateCalls(
            StreamExecWindowAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecWindowAggregate.class, "aggCalls"))
                .clone();
    }

    private static WindowingStrategy windowing(StreamExecWindowAggregate aggregate) {
        return (WindowingStrategy) field(aggregate, StreamExecWindowAggregate.class, "windowing");
    }

    private static NamedWindowProperty[] windowProperties(StreamExecWindowAggregate aggregate) {
        return ((NamedWindowProperty[]) field(aggregate, StreamExecWindowAggregate.class, "namedWindowProperties"))
                .clone();
    }

    private static boolean windowNeedRetraction(StreamExecWindowAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecWindowAggregate.class, "needRetraction");
    }

    private static TwoPhaseWindowAggregate twoPhaseWindowAggregate(StreamExecGlobalWindowAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> exchange = global.getInputEdges().get(0).getSource();
        if (!(exchange instanceof StreamExecExchange)
                || exchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> local = exchange.getInputEdges().get(0).getSource();
        if (!(local instanceof StreamExecLocalWindowAggregate)
                || local.getInputEdges().size() != 1) {
            return null;
        }
        return new TwoPhaseWindowAggregate(
                global,
                (StreamExecLocalWindowAggregate) local,
                local.getInputEdges().get(0));
    }

    /**
     * Flink materializes {@code PROCTIME()} in a Calc directly below the distribution required by
     * a one-phase processing-time window. The native window operator reads Flink's processing-time
     * service instead of that synthetic column, so fold the Calc and its exchange while remapping
     * every real projected field back to the original input.
     */
    private static ProcessingTimeWindowAggregate processingTimeWindowAggregate(StreamExecWindowAggregate node) {
        WindowingStrategy originalWindowing = windowing(node);
        if (!(originalWindowing instanceof TimeAttributeWindowingStrategy) || !originalWindowing.isProctime()) {
            return null;
        }
        ExecEdge aggregateInput = node.getInputEdges().get(0);
        if (!(aggregateInput.getSource() instanceof StreamExecExchange)) {
            return null;
        }
        StreamExecExchange exchange = (StreamExecExchange) aggregateInput.getSource();
        ExecEdge exchangeInput = exchange.getInputEdges().get(0);
        if (!(exchangeInput.getSource() instanceof StreamExecCalc)) {
            return null;
        }
        StreamExecCalc calc = (StreamExecCalc) exchangeInput.getSource();
        if (condition(calc) != null) {
            return null;
        }
        List<RexNode> projects = projection(calc);
        int timeIndex = ((TimeAttributeWindowingStrategy) originalWindowing).getTimeAttributeIndex();
        if (timeIndex < 0 || timeIndex >= projects.size() || !isProctimeCall(projects.get(timeIndex))) {
            return null;
        }
        int[] sourceIndex = new int[projects.size()];
        java.util.Arrays.fill(sourceIndex, -1);
        for (int index = 0; index < projects.size(); index++) {
            RexNode project = projects.get(index);
            if (index == timeIndex) {
                continue;
            }
            if (!(project instanceof RexInputRef)) {
                return null;
            }
            sourceIndex[index] = ((RexInputRef) project).getIndex();
        }
        int[] remappedGrouping = windowGrouping(node);
        for (int index = 0; index < remappedGrouping.length; index++) {
            int projected = remappedGrouping[index];
            if (projected < 0 || projected >= sourceIndex.length || sourceIndex[projected] < 0) {
                return null;
            }
            remappedGrouping[index] = sourceIndex[projected];
        }
        org.apache.calcite.rel.core.AggregateCall[] calls = windowAggregateCalls(node);
        for (int index = 0; index < calls.length; index++) {
            List<Integer> remappedArguments =
                    new ArrayList<>(calls[index].getArgList().size());
            for (int projected : calls[index].getArgList()) {
                if (projected < 0 || projected >= sourceIndex.length || sourceIndex[projected] < 0) {
                    return null;
                }
                remappedArguments.add(sourceIndex[projected]);
            }
            calls[index] = calls[index].withArgList(remappedArguments);
        }
        TimeAttributeWindowingStrategy remappedWindowing = new TimeAttributeWindowingStrategy(
                originalWindowing.getWindow(), originalWindowing.getTimeAttributeType(), 0);
        ExecEdge calcInput = calc.getInputEdges().get(0);
        return new ProcessingTimeWindowAggregate(
                node, calcInput, calc.getInputProperties().get(0), remappedGrouping, calls, remappedWindowing);
    }

    private static boolean isProctimeCall(RexNode expression) {
        return expression instanceof RexCall
                && ((RexCall) expression).getOperator().getName().equalsIgnoreCase("PROCTIME");
    }

    private static int[] localWindowGrouping(StreamExecLocalWindowAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecLocalWindowAggregate.class, "grouping")).clone();
    }

    private static org.apache.calcite.rel.core.AggregateCall[] localWindowAggregateCalls(
            StreamExecLocalWindowAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecLocalWindowAggregate.class, "aggCalls"))
                .clone();
    }

    private static WindowingStrategy localWindowing(StreamExecLocalWindowAggregate aggregate) {
        return (WindowingStrategy) field(aggregate, StreamExecLocalWindowAggregate.class, "windowing");
    }

    private static NamedWindowProperty[] globalWindowProperties(StreamExecGlobalWindowAggregate aggregate) {
        return ((NamedWindowProperty[])
                        field(aggregate, StreamExecGlobalWindowAggregate.class, "namedWindowProperties"))
                .clone();
    }

    private static boolean globalWindowNeedRetraction(StreamExecGlobalWindowAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecGlobalWindowAggregate.class, "needRetraction");
    }

    private static final class TwoPhaseWindowAggregate {
        private final StreamExecGlobalWindowAggregate global;
        private final StreamExecLocalWindowAggregate local;
        private final ExecEdge inputEdge;

        private TwoPhaseWindowAggregate(
                StreamExecGlobalWindowAggregate global, StreamExecLocalWindowAggregate local, ExecEdge inputEdge) {
            this.global = global;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }

    private static final class ProcessingTimeWindowAggregate {
        private final StreamExecWindowAggregate node;
        private final ExecEdge inputEdge;
        private final org.apache.flink.table.planner.plan.nodes.exec.InputProperty inputProperty;
        private final int[] grouping;
        private final org.apache.calcite.rel.core.AggregateCall[] aggregateCalls;
        private final TimeAttributeWindowingStrategy windowing;

        private ProcessingTimeWindowAggregate(
                StreamExecWindowAggregate node,
                ExecEdge inputEdge,
                org.apache.flink.table.planner.plan.nodes.exec.InputProperty inputProperty,
                int[] grouping,
                org.apache.calcite.rel.core.AggregateCall[] aggregateCalls,
                TimeAttributeWindowingStrategy windowing) {
            this.node = node;
            this.inputEdge = inputEdge;
            this.inputProperty = inputProperty;
            this.grouping = grouping;
            this.aggregateCalls = aggregateCalls;
            this.windowing = windowing;
        }
    }

    private static Object field(Object node, Class<?> declaringClass, String name) {
        try {
            Field field = declaringClass.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(node);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Could not read Flink exec node " + name, e);
        }
    }
}
