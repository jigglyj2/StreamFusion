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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalSort;
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
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.VariableRankRange;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.TimeUtils;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.FixedMatchRecognize;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.ProcessingTimeMatchRecognize;

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
    private static final String GROUP_WINDOW_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionGroupWindowAggregateTranslator";
    private static final String WINDOW_DEDUPLICATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowDeduplicateTranslator";
    private static final String WINDOW_RANK_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowRankTranslator";
    private static final String TOP_N_TRANSLATOR_CLASS = "tech.streamfusion.flink.topn.StreamFusionTopNTranslator";
    private static final String WINDOW_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowJoinTranslator";
    private static final String REGULAR_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionRegularJoinTranslator";
    private static final String MULTI_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionMultiJoinTranslator";
    private static final String INTERVAL_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionIntervalJoinTranslator";
    private static final String TEMPORAL_JOIN_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.join.StreamFusionTemporalJoinTranslator";
    private static final String OVER_AGGREGATE_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.over.StreamFusionOverAggregateTranslator";
    private static final String TEMPORAL_SORT_TRANSLATOR_CLASS =
            "tech.streamfusion.flink.sort.StreamFusionTemporalSortTranslator";
    private transient ReadableConfig activeTableConfig;

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        StreamFusionPlanningDiagnostics.begin();
        // Focused graph-shape tests construct the processor without a planner. Persisted node
        // configuration remains sufficient unless a translated mini-batch node omitted its size.
        activeTableConfig = context == null ? null : context.getPlanner().getTableConfig();
        try {
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
        } finally {
            activeTableConfig = null;
        }
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
            StreamExecCalc calc = (StreamExecCalc) node;
            ProcessingTimeDeduplicate foldedDeduplicate = processingTimeDeduplicate(calc);
            ProcessingTimeOverAggregate folded = foldedDeduplicate == null ? processingTimeOverAggregate(calc) : null;
            String reason = foldedDeduplicate != null
                    ? unsupportedReason(foldedDeduplicate, context)
                    : folded == null ? unsupportedReason(calc, context) : unsupportedReason(folded, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (foldedDeduplicate != null) {
                collectRejections(
                        foldedDeduplicate.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            if (folded != null) {
                collectRejections(folded.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
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
        } else if (node instanceof StreamExecGlobalGroupAggregate) {
            IncrementalGroupAggregate incremental = incrementalGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (incremental != null) {
                String reason = unsupportedReason(incremental, context);
                if (reason != null) {
                    rejections.add(nodePath + "\n" + reason);
                }
                collectRejections(incremental.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            if (hasIncrementalGroupAggregateChain((StreamExecGlobalGroupAggregate) node)) {
                rejections.add(nodePath
                        + "\nincremental group aggregate: expanded or computed DISTINCT split input "
                        + "requires dedicated native incremental stages");
                return;
            }
            TwoPhaseGroupAggregate twoPhase = twoPhaseGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (twoPhase == null) {
                rejections.add(nodePath
                        + "\nglobal group aggregate: expected LocalGroupAggregate -> Exchange -> "
                        + "GlobalGroupAggregate");
                return;
            }
            String reason = unsupportedReason(twoPhase, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(twoPhase.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof StreamExecLocalGroupAggregate) {
            rejections.add(
                    nodePath + "\nlocal group aggregate: native acceleration requires its paired global aggregate");
        } else if (node instanceof StreamExecGroupAggregate) {
            String reason = unsupportedReason((StreamExecGroupAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecGroupWindowAggregate) {
            LegacyGroupWindowAggregate legacy = legacyGroupWindowAggregate((StreamExecGroupWindowAggregate) node);
            String reason = unsupportedReason(legacy, context);
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
        } else if (node instanceof StreamExecTemporalJoin) {
            String reason = unsupportedReason((StreamExecTemporalJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecIntervalJoin) {
            String reason = unsupportedReason((StreamExecIntervalJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecMultiJoin) {
            String reason = unsupportedReason((StreamExecMultiJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecJoin) {
            String reason = unsupportedReason((StreamExecJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecMatch) {
            ProcessingTimeMatchRecognize folded =
                    StreamFusionMatchRecognizePlanner.processingTimeMatchRecognize((StreamExecMatch) node);
            FixedMatchRecognize match = folded == null
                    ? FixedMatchRecognize.rejected(
                            (StreamExecMatch) node,
                            "processing time: expected Calc(PROCTIME) -> Exchange -> Match physical shape")
                    : folded.match;
            String reason = match.rejectionReason != null
                    ? match.rejectionReason
                    : StreamFusionMatchRecognizePlanner.unsupportedReason(match, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (folded != null && reason == null) {
                String calcReason = unsupportedCalcReason(
                        (RowType) folded.inputEdge.getOutputType(),
                        folded.inputType,
                        folded.inputProjection,
                        folded.inputCondition,
                        context);
                if (calcReason != null) {
                    rejections.add(nodePath + "/native-input-calc\n" + calcReason);
                }
                collectRejections(folded.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof StreamExecDropUpdateBefore) {
            // RowKind is Flink changelog metadata, so this node is always eligible.
        } else if (node instanceof StreamExecMiniBatchAssigner) {
            // Native stateful operators already consume Arrow mini-batches. The latency-marker
            // assigner is folded into the native stateful node during conversion.
        } else if (node instanceof StreamExecTemporalSort) {
            String reason = unsupportedReason((StreamExecTemporalSort) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
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
            ProcessingTimeDeduplicate foldedDeduplicate = processingTimeDeduplicate(calc);
            if (foldedDeduplicate != null) {
                StreamFusionExecCalc inputProjection = new StreamFusionExecCalc(
                        foldedDeduplicate.inputCalc.getPersistedConfig(),
                        foldedDeduplicate.inputProjection,
                        condition(foldedDeduplicate.inputCalc),
                        foldedDeduplicate.inputCalc.getInputProperties().get(0),
                        foldedDeduplicate.inputType,
                        "StreamFusionCalc");
                inputProjection.setInputEdges(List.of(copyEdge(
                        foldedDeduplicate.inputEdge,
                        convert(foldedDeduplicate.inputEdge.getSource()),
                        inputProjection)));

                StreamFusionExecExchange exchange = new StreamFusionExecExchange(
                        foldedDeduplicate.exchange.getPersistedConfig(),
                        foldedDeduplicate.exchange.getInputProperties().get(0),
                        foldedDeduplicate.inputType,
                        "StreamFusionExchange");
                exchange.setInputEdges(List.of(
                        copyEdge(foldedDeduplicate.exchange.getInputEdges().get(0), inputProjection, exchange)));

                StreamFusionExecDeduplicate deduplicate = new StreamFusionExecDeduplicate(
                        foldedDeduplicate.deduplicate.getPersistedConfig(),
                        foldedDeduplicate.uniqueKeys,
                        false,
                        booleanField(foldedDeduplicate.deduplicate, "keepLastRow"),
                        booleanField(foldedDeduplicate.deduplicate, "outputInsertOnly"),
                        booleanField(foldedDeduplicate.deduplicate, "generateUpdateBefore"),
                        stateMetadata(foldedDeduplicate.deduplicate),
                        foldedDeduplicate.deduplicate.getInputProperties().get(0),
                        foldedDeduplicate.deduplicateOutputType,
                        "StreamFusionDeduplicate");
                deduplicate.setInputEdges(List.of(
                        copyEdge(foldedDeduplicate.deduplicate.getInputEdges().get(0), exchange, deduplicate)));

                StreamFusionExecCalc replacement = new StreamFusionExecCalc(
                        calc.getPersistedConfig(),
                        foldedDeduplicate.outputProjection,
                        foldedDeduplicate.outputCondition,
                        calc.getInputProperties().get(0),
                        (RowType) calc.getOutputType(),
                        "StreamFusionCalc");
                replacement.setInputEdges(List.of(ExecEdge.builder()
                        .source(deduplicate)
                        .target(replacement)
                        .shuffle(ExecEdge.FORWARD_SHUFFLE)
                        .build()));
                return replacement;
            }
            ProcessingTimeOverAggregate folded = processingTimeOverAggregate(calc);
            if (folded != null) {
                StreamFusionExecCalc inputProjection = new StreamFusionExecCalc(
                        folded.inputCalc.getPersistedConfig(),
                        folded.inputProjection,
                        condition(folded.inputCalc),
                        folded.inputCalc.getInputProperties().get(0),
                        folded.inputType,
                        "StreamFusionCalc");
                inputProjection.setInputEdges(
                        List.of(copyEdge(folded.inputEdge, convert(folded.inputEdge.getSource()), inputProjection)));
                StreamFusionExecOverAggregate over = new StreamFusionExecOverAggregate(
                        folded.aggregate.getPersistedConfig(),
                        folded.overSpec,
                        folded.inputCalc.getInputProperties().get(0),
                        folded.overOutputType,
                        "StreamFusionOverAggregate",
                        true);
                over.setInputEdges(List.of(ExecEdge.builder()
                        .source(inputProjection)
                        .target(over)
                        .shuffle(ExecEdge.FORWARD_SHUFFLE)
                        .build()));
                StreamFusionExecCalc replacement = new StreamFusionExecCalc(
                        calc.getPersistedConfig(),
                        folded.outputProjection,
                        folded.outputCondition,
                        calc.getInputProperties().get(0),
                        (RowType) calc.getOutputType(),
                        "StreamFusionCalc");
                replacement.setInputEdges(List.of(ExecEdge.builder()
                        .source(over)
                        .target(replacement)
                        .shuffle(ExecEdge.FORWARD_SHUFFLE)
                        .build()));
                return replacement;
            }
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
        if (node instanceof StreamExecGlobalGroupAggregate) {
            IncrementalGroupAggregate incremental = incrementalGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (incremental != null) {
                RowType localInternalType = nativeGroupAccumulatorType(
                        (RowType) incremental.inputEdge.getOutputType(), localGroupGrouping(incremental.local));
                StreamFusionExecLocalGroupAggregate local = new StreamFusionExecLocalGroupAggregate(
                        incremental.local.getPersistedConfig(),
                        localGroupGrouping(incremental.local),
                        localGroupAggregateCalls(incremental.local),
                        localGroupCallNeedRetractions(incremental.local),
                        localGroupNeedRetraction(incremental.local),
                        miniBatchSize(incremental.local),
                        incremental.local.getInputProperties().get(0),
                        localInternalType);
                local.setInputEdges(
                        List.of(copyEdge(incremental.inputEdge, convert(incremental.inputEdge.getSource()), local)));

                StreamFusionExecExchange partialExchange = new StreamFusionExecExchange(
                        incremental.partialExchange.getPersistedConfig(),
                        incremental.partialExchange.getInputProperties().get(0),
                        localInternalType,
                        "StreamFusionExchange");
                partialExchange.setInputEdges(List.of(
                        copyEdge(incremental.partialExchange.getInputEdges().get(0), local, partialExchange)));

                int[] finalGrouping = incrementalFinalGrouping(incremental.incremental);
                RowType incrementalInternalType = nativeGroupingAccumulatorType(localInternalType, finalGrouping);
                StreamFusionExecIncrementalGroupAggregate nativeIncremental =
                        new StreamFusionExecIncrementalGroupAggregate(
                                incremental.incremental.getPersistedConfig(),
                                incrementalPartialOriginalInputType(incremental.incremental),
                                localGroupGrouping(incremental.local).length,
                                finalGrouping,
                                incrementalOriginalCalls(incremental.incremental),
                                incrementalCallNeedRetractions(incremental.incremental),
                                globalGroupOriginalInputType(incremental.global),
                                globalGroupAggregateCalls(incremental.global),
                                globalGroupCallNeedRetractions(incremental.global),
                                miniBatchSize(incremental.incremental),
                                incremental.incremental.getInputProperties().get(0),
                                incrementalInternalType);
                nativeIncremental.setInputEdges(List.of(
                        copyEdge(incremental.incremental.getInputEdges().get(0), partialExchange, nativeIncremental)));

                StreamFusionExecExchange finalExchange = new StreamFusionExecExchange(
                        incremental.finalExchange.getPersistedConfig(),
                        incremental.finalExchange.getInputProperties().get(0),
                        incrementalInternalType,
                        "StreamFusionExchange");
                finalExchange.setInputEdges(List.of(
                        copyEdge(incremental.finalExchange.getInputEdges().get(0), nativeIncremental, finalExchange)));

                StreamFusionExecGlobalGroupAggregate replacement = new StreamFusionExecGlobalGroupAggregate(
                        incremental.global.getPersistedConfig(),
                        globalGroupOriginalInputType(incremental.global),
                        finalGrouping.length,
                        globalGroupAggregateCalls(incremental.global),
                        globalGroupCallNeedRetractions(incremental.global),
                        globalGroupGenerateUpdateBefore(incremental.global),
                        incremental.global.getInputProperties().get(0),
                        (RowType) incremental.global.getOutputType());
                replacement.setInputEdges(
                        List.of(copyEdge(incremental.global.getInputEdges().get(0), finalExchange, replacement)));
                return replacement;
            }
            TwoPhaseGroupAggregate twoPhase = twoPhaseGroupAggregate((StreamExecGlobalGroupAggregate) node);
            if (twoPhase == null) {
                throw new IllegalStateException("Selected malformed two-phase group aggregate");
            }
            RowType internalType = nativeGroupAccumulatorType(
                    (RowType) twoPhase.inputEdge.getOutputType(), localGroupGrouping(twoPhase.local));
            StreamFusionExecLocalGroupAggregate local = new StreamFusionExecLocalGroupAggregate(
                    twoPhase.local.getPersistedConfig(),
                    localGroupGrouping(twoPhase.local),
                    localGroupAggregateCalls(twoPhase.local),
                    localGroupCallNeedRetractions(twoPhase.local),
                    localGroupNeedRetraction(twoPhase.local),
                    miniBatchSize(twoPhase.local),
                    twoPhase.local.getInputProperties().get(0),
                    internalType);
            local.setInputEdges(List.of(copyEdge(twoPhase.inputEdge, convert(twoPhase.inputEdge.getSource()), local)));

            StreamFusionExecExchange exchange = new StreamFusionExecExchange(
                    twoPhase.exchange.getPersistedConfig(),
                    twoPhase.exchange.getInputProperties().get(0),
                    internalType,
                    "StreamFusionExchange");
            exchange.setInputEdges(
                    List.of(copyEdge(twoPhase.exchange.getInputEdges().get(0), local, exchange)));

            StreamFusionExecGlobalGroupAggregate global = new StreamFusionExecGlobalGroupAggregate(
                    twoPhase.global.getPersistedConfig(),
                    globalGroupOriginalInputType(twoPhase.global),
                    localGroupGrouping(twoPhase.local).length,
                    globalGroupAggregateCalls(twoPhase.global),
                    globalGroupCallNeedRetractions(twoPhase.global),
                    globalGroupGenerateUpdateBefore(twoPhase.global),
                    twoPhase.global.getInputProperties().get(0),
                    (RowType) twoPhase.global.getOutputType());
            global.setInputEdges(
                    List.of(copyEdge(twoPhase.global.getInputEdges().get(0), exchange, global)));
            return global;
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
        if (node instanceof StreamExecGroupWindowAggregate) {
            StreamExecGroupWindowAggregate aggregate = (StreamExecGroupWindowAggregate) node;
            LegacyGroupWindowAggregate legacy = legacyGroupWindowAggregate(aggregate);
            StreamFusionExecGroupWindowAggregate replacement = new StreamFusionExecGroupWindowAggregate(
                    activeTableConfig == null ? aggregate.getPersistedConfig() : activeTableConfig,
                    legacy.grouping,
                    legacy.aggregateCalls,
                    legacy.window,
                    legacy.properties,
                    legacy.needRetraction,
                    aggregate.getInputProperties().get(0),
                    (RowType) aggregate.getOutputType(),
                    "StreamFusionGroupWindowAggregate");
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
                    "StreamFusionOverAggregate",
                    false);
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
        if (node instanceof StreamExecMultiJoin) {
            StreamExecMultiJoin join = (StreamExecMultiJoin) node;
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
        if (node instanceof StreamExecMatch) {
            StreamExecMatch match = (StreamExecMatch) node;
            ProcessingTimeMatchRecognize folded = StreamFusionMatchRecognizePlanner.processingTimeMatchRecognize(match);
            if (folded == null || folded.match.rejectionReason != null) {
                throw new IllegalStateException("Selected unsupported MATCH_RECOGNIZE physical shape");
            }
            StreamFusionExecCalc inputProjection = new StreamFusionExecCalc(
                    folded.inputCalc.getPersistedConfig(),
                    folded.inputProjection,
                    folded.inputCondition,
                    folded.inputCalc.getInputProperties().get(0),
                    folded.inputType,
                    "StreamFusionCalc");
            inputProjection.setInputEdges(
                    List.of(copyEdge(folded.inputEdge, convert(folded.inputEdge.getSource()), inputProjection)));
            StreamFusionExecExchange exchange = new StreamFusionExecExchange(
                    folded.exchange.getPersistedConfig(),
                    folded.exchange.getInputProperties().get(0),
                    folded.inputType,
                    "StreamFusionExchange");
            exchange.setInputEdges(
                    List.of(copyEdge(folded.exchange.getInputEdges().get(0), inputProjection, exchange)));
            FixedMatchRecognize fixed = folded.match;
            StreamFusionExecMatchRecognize replacement = new StreamFusionExecMatchRecognize(
                    match.getPersistedConfig(),
                    fixed.partitionKeys,
                    fixed.variableNames,
                    fixed.conditions,
                    fixed.measureVariables,
                    fixed.measureFields,
                    fixed.skipPastLastRow,
                    match.getInputProperties().get(0),
                    (RowType) match.getOutputType(),
                    "StreamFusionMatchRecognize");
            replacement.setInputEdges(List.of(copyEdge(match.getInputEdges().get(0), exchange, replacement)));
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
            StreamExecMiniBatchAssigner assigner = (StreamExecMiniBatchAssigner) node;
            StreamFusionExecMiniBatchAssigner replacement = new StreamFusionExecMiniBatchAssigner(
                    assigner.getPersistedConfig(),
                    miniBatchInterval(assigner),
                    assigner.getInputProperties().get(0),
                    (RowType) assigner.getOutputType(),
                    "StreamFusionMiniBatchAssigner");
            replacement.setInputEdges(assigner.getInputEdges().stream()
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
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
                    .map(edge -> copyEdge(edge, convert(edge.getSource()), replacement))
                    .collect(Collectors.toList()));
            return replacement;
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

    private long miniBatchSize(ExecNodeBase<?> node) {
        return node.getPersistedConfig()
                .getOptional(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE)
                .orElseGet(() -> activeTableConfig == null
                        ? ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE.defaultValue()
                        : activeTableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE));
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
        return unsupportedCalcReason(
                (RowType) input.getOutputType(),
                (RowType) calc.getOutputType(),
                projection(calc),
                condition(calc),
                context);
    }

    private String unsupportedCalcReason(
            RowType inputType,
            RowType outputType,
            List<RexNode> projections,
            RexNode condition,
            ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method =
                    translator.getMethod("unsupportedReason", RowType.class, RowType.class, List.class, Object.class);
            return (String) method.invoke(null, inputType, outputType, projections, condition);
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

    private String unsupportedReason(StreamExecTemporalJoin join, ProcessorContext context) {
        ExecEdge left = join.getInputEdges().get(0);
        ExecEdge right = join.getInputEdges().get(1);
        try {
            Class<?> translator = Class.forName(
                    TEMPORAL_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    boolean.class,
                    int.class,
                    int.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) left.getOutputType(),
                    (RowType) right.getOutputType(),
                    (RowType) join.getOutputType(),
                    temporalJoinSpec(join),
                    temporalJoinFunction(join),
                    temporalJoinLeftTimeIndex(join),
                    temporalJoinRightTimeIndex(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion temporal join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion temporal join support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(StreamExecMultiJoin join, ProcessorContext context) {
        List<RowType> inputTypes = join.getInputEdges().stream()
                .map(edge -> (RowType) edge.getOutputType())
                .collect(Collectors.toList());
        Map<Integer, List<ConditionAttributeRef>> attributes = multiJoinAttributeMap(join);
        org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor extractor =
                new org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor(
                        attributes, inputTypes);
        List<int[]> commonKeys = java.util.stream.IntStream.range(0, inputTypes.size())
                .mapToObj(extractor::getCommonJoinKeyIndices)
                .collect(Collectors.toList());
        try {
            Class<?> translator = Class.forName(
                    MULTI_JOIN_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    List.class,
                    RowType.class,
                    List.class,
                    List.class,
                    Map.class,
                    List.class,
                    long[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    inputTypes,
                    (RowType) join.getOutputType(),
                    commonKeys,
                    multiJoinTypes(join),
                    attributes,
                    multiJoinUniqueKeys(join),
                    multiJoinStateTtl(join),
                    multiJoinEquiOnly(join),
                    join.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion multi-join support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion multi-join support inspection failed", e.getCause());
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

    private String unsupportedReason(TwoPhaseGroupAggregate aggregate, ProcessorContext context) {
        StreamExecLocalGroupAggregate local = aggregate.local;
        StreamExecGlobalGroupAggregate global = aggregate.global;
        int[] expectedGlobalGrouping = java.util.stream.IntStream.range(0, localGroupGrouping(local).length)
                .toArray();
        if (!java.util.Arrays.equals(globalGroupGrouping(global), expectedGlobalGrouping)) {
            return "two-phase aggregate: global grouping must address the local grouping prefix";
        }
        if (localGroupAggregateCalls(local).length != globalGroupAggregateCalls(global).length) {
            return "two-phase aggregate: local and global aggregate call counts differ";
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
                    (RowType) aggregate.inputEdge.getOutputType(),
                    (RowType) global.getOutputType(),
                    localGroupGrouping(local),
                    localGroupAggregateCalls(local),
                    localGroupCallNeedRetractions(local),
                    globalGroupGenerateUpdateBefore(global),
                    localGroupNeedRetraction(local),
                    globalGroupStateTtl(global),
                    global.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect two-phase GroupAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Two-phase GroupAggregate support inspection failed", e.getCause());
        }
    }

    private String unsupportedReason(IncrementalGroupAggregate aggregate, ProcessorContext context) {
        RowType partialInput = incrementalPartialOriginalInputType(aggregate.incremental);
        org.apache.calcite.rel.core.AggregateCall[] partialCalls = incrementalOriginalCalls(aggregate.incremental);
        String partialReason = groupAggregateUnsupported(
                context,
                partialInput,
                aggregateOutputType(partialInput, localGroupGrouping(aggregate.local), partialCalls),
                localGroupGrouping(aggregate.local),
                partialCalls,
                incrementalCallNeedRetractions(aggregate.incremental),
                false,
                incrementalNeedRetraction(aggregate.incremental),
                incrementalStateTtl(aggregate.incremental),
                aggregate.incremental.getPersistedConfig());
        if (partialReason != null) {
            return "incremental partial " + partialReason;
        }
        String finalReason = groupAggregateUnsupported(
                context,
                globalGroupOriginalInputType(aggregate.global),
                (RowType) aggregate.global.getOutputType(),
                globalGroupGrouping(aggregate.global),
                globalGroupAggregateCalls(aggregate.global),
                globalGroupCallNeedRetractions(aggregate.global),
                globalGroupGenerateUpdateBefore(aggregate.global),
                globalGroupNeedRetraction(aggregate.global),
                globalGroupStateTtl(aggregate.global),
                aggregate.global.getPersistedConfig());
        return finalReason == null ? null : "incremental final " + finalReason;
    }

    private String groupAggregateUnsupported(
            ProcessorContext context,
            RowType inputType,
            RowType outputType,
            int[] grouping,
            org.apache.calcite.rel.core.AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean needRetraction,
            long stateTtl,
            ReadableConfig config) {
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
                    inputType,
                    outputType,
                    grouping,
                    calls,
                    retractable,
                    generateUpdateBefore,
                    needRetraction,
                    stateTtl,
                    config);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect incremental GroupAggregate support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Incremental GroupAggregate support inspection failed", e.getCause());
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

    private String unsupportedReason(ProcessingTimeDeduplicate folded, ProcessorContext context) {
        String reason = unsupportedCalcReason(
                (RowType) folded.inputEdge.getOutputType(),
                folded.inputType,
                folded.inputProjection,
                condition(folded.inputCalc),
                context);
        if (reason != null) {
            return "processing-time input projection: " + reason;
        }
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
            reason = (String) method.invoke(
                    null,
                    folded.inputType,
                    folded.deduplicateOutputType,
                    folded.uniqueKeys,
                    false,
                    booleanField(folded.deduplicate, "keepLastRow"),
                    booleanField(folded.deduplicate, "outputInsertOnly"),
                    booleanField(folded.deduplicate, "generateUpdateBefore"),
                    stateTtl(folded.deduplicate),
                    folded.deduplicate.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect folded processing-time deduplicate support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Folded processing-time deduplicate support inspection failed", failure.getCause());
        }
        if (reason != null) {
            return reason;
        }
        reason = unsupportedCalcReason(
                folded.deduplicateOutputType,
                (RowType) folded.outputCalc.getOutputType(),
                folded.outputProjection,
                folded.outputCondition,
                context);
        return reason == null ? null : "processing-time output projection: " + reason;
    }

    private String unsupportedReason(ProcessingTimeOverAggregate folded, ProcessorContext context) {
        String reason = unsupportedCalcReason(
                (RowType) folded.inputEdge.getOutputType(),
                folded.inputType,
                folded.inputProjection,
                condition(folded.inputCalc),
                context);
        if (reason != null) {
            return "processing-time input projection: " + reason;
        }
        long stateTtl = folded.aggregate
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
                    ReadableConfig.class,
                    boolean.class);
            reason = (String) method.invoke(
                    null,
                    folded.inputType,
                    folded.overOutputType,
                    folded.overSpec,
                    stateTtl,
                    folded.aggregate.getPersistedConfig(),
                    true);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect folded processing-time OVER support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException(
                    "Folded processing-time OVER support inspection failed", failure.getCause());
        }
        if (reason != null) {
            return reason;
        }
        reason = unsupportedCalcReason(
                folded.overOutputType,
                (RowType) folded.outputCalc.getOutputType(),
                folded.outputProjection,
                folded.outputCondition,
                context);
        return reason == null ? null : "processing-time output projection: " + reason;
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

    private String unsupportedReason(LegacyGroupWindowAggregate aggregate, ProcessorContext context) {
        try {
            Class<?> translator = Class.forName(
                    GROUP_WINDOW_AGGREGATE_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    org.apache.calcite.rel.core.AggregateCall[].class,
                    LogicalWindow.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) aggregate.node.getInputEdges().get(0).getOutputType(),
                    (RowType) aggregate.node.getOutputType(),
                    aggregate.grouping,
                    aggregate.aggregateCalls,
                    aggregate.window,
                    aggregate.properties,
                    aggregate.needRetraction,
                    activeTableConfig == null ? aggregate.node.getPersistedConfig() : activeTableConfig);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect legacy group-window support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Legacy group-window support inspection failed", e.getCause());
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

    private String unsupportedReason(StreamExecTemporalSort sort, ProcessorContext context) {
        SortSpec sortSpec = temporalSortSpec(sort);
        try {
            Class<?> translator = Class.forName(
                    TEMPORAL_SORT_TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason", RowType.class, SortSpec.class, boolean.class, ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) sort.getInputEdges().get(0).getOutputType(),
                    sortSpec,
                    temporalSortProcessingTime(sort, sortSpec),
                    sort.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not inspect StreamFusion TemporalSort support", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion TemporalSort support inspection failed", failure.getCause());
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

    private static int[] localGroupGrouping(StreamExecLocalGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecLocalGroupAggregate.class, "grouping")).clone();
    }

    private static org.apache.calcite.rel.core.AggregateCall[] localGroupAggregateCalls(
            StreamExecLocalGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecLocalGroupAggregate.class, "aggCalls"))
                .clone();
    }

    private static boolean[] localGroupCallNeedRetractions(StreamExecLocalGroupAggregate aggregate) {
        return ((boolean[]) field(aggregate, StreamExecLocalGroupAggregate.class, "aggCallNeedRetractions")).clone();
    }

    private static boolean localGroupNeedRetraction(StreamExecLocalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecLocalGroupAggregate.class, "needRetraction");
    }

    private static int[] globalGroupGrouping(StreamExecGlobalGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecGlobalGroupAggregate.class, "grouping")).clone();
    }

    private static org.apache.calcite.rel.core.AggregateCall[] globalGroupAggregateCalls(
            StreamExecGlobalGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecGlobalGroupAggregate.class, "aggCalls"))
                .clone();
    }

    private static boolean[] globalGroupCallNeedRetractions(StreamExecGlobalGroupAggregate aggregate) {
        return ((boolean[]) field(aggregate, StreamExecGlobalGroupAggregate.class, "aggCallNeedRetractions")).clone();
    }

    private static boolean globalGroupNeedRetraction(StreamExecGlobalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecGlobalGroupAggregate.class, "needRetraction");
    }

    private static boolean globalGroupGenerateUpdateBefore(StreamExecGlobalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecGlobalGroupAggregate.class, "generateUpdateBefore");
    }

    @SuppressWarnings("unchecked")
    private static List<StateMetadata> globalGroupStateMetadata(StreamExecGlobalGroupAggregate aggregate) {
        return (List<StateMetadata>) field(aggregate, StreamExecGlobalGroupAggregate.class, "stateMetadataList");
    }

    private static RowType globalGroupOriginalInputType(StreamExecGlobalGroupAggregate aggregate) {
        return (RowType) field(aggregate, StreamExecGlobalGroupAggregate.class, "localAggInputRowType");
    }

    @SuppressWarnings("unchecked")
    private static long globalGroupStateTtl(StreamExecGlobalGroupAggregate aggregate) {
        List<StateMetadata> metadata = globalGroupStateMetadata(aggregate);
        if (metadata == null || metadata.isEmpty()) {
            return aggregate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    private static org.apache.calcite.rel.core.AggregateCall[] incrementalOriginalCalls(
            StreamExecIncrementalGroupAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialOriginalAggCalls"))
                .clone();
    }

    private static int[] incrementalFinalGrouping(StreamExecIncrementalGroupAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecIncrementalGroupAggregate.class, "finalAggGrouping")).clone();
    }

    private static RowType incrementalPartialOriginalInputType(StreamExecIncrementalGroupAggregate aggregate) {
        return (RowType) field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialLocalAggInputType");
    }

    private static boolean[] incrementalCallNeedRetractions(StreamExecIncrementalGroupAggregate aggregate) {
        return ((boolean[])
                        field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialAggCallNeedRetractions"))
                .clone();
    }

    private static boolean incrementalNeedRetraction(StreamExecIncrementalGroupAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecIncrementalGroupAggregate.class, "partialAggNeedRetraction");
    }

    @SuppressWarnings("unchecked")
    private static long incrementalStateTtl(StreamExecIncrementalGroupAggregate aggregate) {
        List<StateMetadata> metadata =
                (List<StateMetadata>) field(aggregate, StreamExecIncrementalGroupAggregate.class, "stateMetadataList");
        if (metadata == null || metadata.isEmpty()) {
            return aggregate
                    .getPersistedConfig()
                    .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                    .toMillis();
        }
        return TimeUtils.parseDuration(metadata.get(0).getStateTtl()).toMillis();
    }

    private static RowType nativeGroupAccumulatorType(RowType inputType, int[] grouping) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + 1);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        fields.add(
                new RowType.RowField("__streamfusion_accumulator", new VarBinaryType(false, VarBinaryType.MAX_LENGTH)));
        return new RowType(false, fields);
    }

    private static RowType nativeGroupingAccumulatorType(RowType inputType, int[] grouping) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + 1);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        fields.add(
                new RowType.RowField("__streamfusion_accumulator", new VarBinaryType(false, VarBinaryType.MAX_LENGTH)));
        return new RowType(false, fields);
    }

    private static RowType aggregateOutputType(
            RowType inputType, int[] grouping, org.apache.calcite.rel.core.AggregateCall[] calls) {
        List<RowType.RowField> fields = new ArrayList<>(grouping.length + calls.length);
        for (int index : grouping) {
            RowType.RowField field = inputType.getFields().get(index);
            fields.add(new RowType.RowField(field.getName(), field.getType()));
        }
        for (int index = 0; index < calls.length; index++) {
            fields.add(new RowType.RowField(
                    "aggregate_" + index,
                    org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(calls[index].getType())));
        }
        return new RowType(false, fields);
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

    private static SortSpec temporalSortSpec(StreamExecTemporalSort sort) {
        return (SortSpec) field(sort, StreamExecTemporalSort.class, "sortSpec");
    }

    private static boolean temporalSortProcessingTime(StreamExecTemporalSort sort, SortSpec sortSpec) {
        int timeIndex = sortSpec.getFieldSpec(0).getFieldIndex();
        RowType inputType = (RowType) sort.getInputEdges().get(0).getOutputType();
        return org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
                inputType.getTypeAt(timeIndex));
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

    @SuppressWarnings("unchecked")
    private static List<FlinkJoinType> multiJoinTypes(StreamExecMultiJoin join) {
        return (List<FlinkJoinType>) field(join, StreamExecMultiJoin.class, "joinTypes");
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, List<ConditionAttributeRef>> multiJoinAttributeMap(StreamExecMultiJoin join) {
        return (Map<Integer, List<ConditionAttributeRef>>) field(join, StreamExecMultiJoin.class, "joinAttributeMap");
    }

    @SuppressWarnings("unchecked")
    private static List<List<int[]>> multiJoinUniqueKeys(StreamExecMultiJoin join) {
        return (List<List<int[]>>) field(join, StreamExecMultiJoin.class, "inputUniqueKeys");
    }

    @SuppressWarnings("unchecked")
    private static List<RexNode> multiJoinConditions(StreamExecMultiJoin join) {
        return (List<RexNode>) field(join, StreamExecMultiJoin.class, "joinConditions");
    }

    private static boolean multiJoinEquiOnly(StreamExecMultiJoin join) {
        List<RexNode> conditions = multiJoinConditions(join);
        Map<Integer, List<ConditionAttributeRef>> attributes = multiJoinAttributeMap(join);
        List<ExecEdge> inputs = join.getInputEdges();
        if (conditions.size() != inputs.size()) {
            return false;
        }
        for (int depth = 1; depth < conditions.size(); depth++) {
            RexNode condition = conditions.get(depth);
            List<RexNode> conjuncts = condition == null ? List.of() : RelOptUtil.conjunctions(condition);
            Set<String> actual = new HashSet<>();
            int leftArity = 0;
            int[] offsets = new int[depth];
            for (int input = 0; input < depth; input++) {
                offsets[input] = leftArity;
                leftArity += ((RowType) inputs.get(input).getOutputType()).getFieldCount();
            }
            for (RexNode conjunct : conjuncts) {
                if (!(conjunct instanceof RexCall)) {
                    return false;
                }
                RexCall call = (RexCall) conjunct;
                if (call.getKind() != org.apache.calcite.sql.SqlKind.EQUALS
                        || call.getOperands().size() != 2
                        || !(call.getOperands().get(0) instanceof RexInputRef)
                        || !(call.getOperands().get(1) instanceof RexInputRef)) {
                    return false;
                }
                int first = ((RexInputRef) call.getOperands().get(0)).getIndex();
                int second = ((RexInputRef) call.getOperands().get(1)).getIndex();
                int left = Math.min(first, second);
                int right = Math.max(first, second);
                if (left >= leftArity || right < leftArity) {
                    return false;
                }
                actual.add(left + ":" + (right - leftArity));
            }
            Set<String> expected = new HashSet<>();
            for (ConditionAttributeRef attribute : attributes.getOrDefault(depth, List.of())) {
                expected.add(
                        (offsets[attribute.leftInputId] + attribute.leftFieldIndex) + ":" + attribute.rightFieldIndex);
            }
            if (!actual.equals(expected)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static long[] multiJoinStateTtl(StreamExecMultiJoin join) {
        int inputCount = join.getInputEdges().size();
        List<StateMetadata> metadata =
                (List<StateMetadata>) field(join, StreamExecMultiJoin.class, "stateMetadataList");
        long[] ttl = new long[inputCount];
        if (metadata == null || metadata.isEmpty()) {
            java.util.Arrays.fill(
                    ttl,
                    join.getPersistedConfig()
                            .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                            .toMillis());
            return ttl;
        }
        boolean[] seen = new boolean[inputCount];
        for (StateMetadata state : metadata) {
            int index = state.getStateIndex();
            if (index < 0 || index >= inputCount || seen[index]) {
                throw new IllegalStateException("Multi-join state TTL indices must be unique and cover every input");
            }
            ttl[index] = TimeUtils.parseDuration(state.getStateTtl()).toMillis();
            seen[index] = true;
        }
        for (boolean present : seen) {
            if (!present) {
                throw new IllegalStateException("Multi-join state TTL must cover every input");
            }
        }
        return ttl;
    }

    private static JoinSpec temporalJoinSpec(StreamExecTemporalJoin join) {
        return (JoinSpec) field(join, StreamExecTemporalJoin.class, "joinSpec");
    }

    private static boolean temporalJoinFunction(StreamExecTemporalJoin join) {
        return (boolean) field(join, StreamExecTemporalJoin.class, "isTemporalFunctionJoin");
    }

    private static int temporalJoinLeftTimeIndex(StreamExecTemporalJoin join) {
        return (int) field(join, StreamExecTemporalJoin.class, "leftTimeAttributeIndex");
    }

    private static int temporalJoinRightTimeIndex(StreamExecTemporalJoin join) {
        return (int) field(join, StreamExecTemporalJoin.class, "rightTimeAttributeIndex");
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

    private static TwoPhaseGroupAggregate twoPhaseGroupAggregate(StreamExecGlobalGroupAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> exchange = global.getInputEdges().get(0).getSource();
        if (!(exchange instanceof StreamExecExchange)
                || exchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> local = exchange.getInputEdges().get(0).getSource();
        if (!(local instanceof StreamExecLocalGroupAggregate)
                || local.getInputEdges().size() != 1) {
            return null;
        }
        return new TwoPhaseGroupAggregate(
                global,
                (StreamExecExchange) exchange,
                (StreamExecLocalGroupAggregate) local,
                local.getInputEdges().get(0));
    }

    private static IncrementalGroupAggregate incrementalGroupAggregate(StreamExecGlobalGroupAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> finalExchange = global.getInputEdges().get(0).getSource();
        if (!(finalExchange instanceof StreamExecExchange)
                || finalExchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> incremental = finalExchange.getInputEdges().get(0).getSource();
        if (!(incremental instanceof StreamExecIncrementalGroupAggregate)
                || incremental.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> partialExchange = incremental.getInputEdges().get(0).getSource();
        if (!(partialExchange instanceof StreamExecExchange)
                || partialExchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> local = partialExchange.getInputEdges().get(0).getSource();
        if (!(local instanceof StreamExecLocalGroupAggregate)
                || local.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge originalInput = local.getInputEdges().get(0);
        return new IncrementalGroupAggregate(
                global,
                (StreamExecExchange) finalExchange,
                (StreamExecIncrementalGroupAggregate) incremental,
                (StreamExecExchange) partialExchange,
                (StreamExecLocalGroupAggregate) local,
                originalInput);
    }

    private static boolean hasIncrementalGroupAggregateChain(StreamExecGlobalGroupAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return false;
        }
        ExecNode<?> finalExchange = global.getInputEdges().get(0).getSource();
        return finalExchange instanceof StreamExecExchange
                && finalExchange.getInputEdges().size() == 1
                && finalExchange.getInputEdges().get(0).getSource() instanceof StreamExecIncrementalGroupAggregate;
    }

    private static org.apache.flink.table.planner.plan.trait.MiniBatchInterval miniBatchInterval(
            StreamExecMiniBatchAssigner assigner) {
        return (org.apache.flink.table.planner.plan.trait.MiniBatchInterval)
                field(assigner, StreamExecMiniBatchAssigner.class, "miniBatchInterval");
    }

    /**
     * Flink materializes {@code PROCTIME()} below an exchange and keeps that synthetic field in an
     * OVER operator's internal output. The native processing-time kernel only needs arrival order.
     * Fold the exchange and synthetic field while retaining native Calcs for the real input and
     * output projections. This is deliberately limited to a parent Calc that does not observe the
     * synthetic field, so an observable processing timestamp remains on Flink.
     */
    private static ProcessingTimeOverAggregate processingTimeOverAggregate(StreamExecCalc outputCalc) {
        if (outputCalc.getInputEdges().size() != 1
                || !(outputCalc.getInputEdges().get(0).getSource() instanceof StreamExecOverAggregate)) {
            return null;
        }
        StreamExecOverAggregate aggregate =
                (StreamExecOverAggregate) outputCalc.getInputEdges().get(0).getSource();
        OverSpec originalSpec = overSpec(aggregate);
        if (originalSpec.getGroups().size() != 1 || aggregate.getInputEdges().size() != 1) {
            return null;
        }
        OverSpec.GroupSpec originalGroup = originalSpec.getGroups().get(0);
        int[] orderFields = originalGroup.getSort().getFieldIndices();
        if (orderFields.length != 1) {
            return null;
        }
        int timeIndex = orderFields[0];
        ExecEdge aggregateInput = aggregate.getInputEdges().get(0);
        if (!(aggregateInput.getSource() instanceof StreamExecExchange)) {
            return null;
        }
        StreamExecExchange exchange = (StreamExecExchange) aggregateInput.getSource();
        if (exchange.getInputEdges().size() != 1
                || !(exchange.getInputEdges().get(0).getSource() instanceof StreamExecCalc)) {
            return null;
        }
        StreamExecCalc inputCalc =
                (StreamExecCalc) exchange.getInputEdges().get(0).getSource();
        if (inputCalc.getInputEdges().size() != 1) {
            return null;
        }
        List<RexNode> originalInputProjection = projection(inputCalc);
        if (timeIndex < 0
                || timeIndex >= originalInputProjection.size()
                || !isProctimeCall(originalInputProjection.get(timeIndex))) {
            return null;
        }
        List<RexNode> inputProjection = new ArrayList<>(originalInputProjection.size() - 1);
        for (int index = 0; index < originalInputProjection.size(); index++) {
            if (index == timeIndex) {
                continue;
            }
            RexNode expression = originalInputProjection.get(index);
            inputProjection.add(expression);
        }
        if (inputProjection.isEmpty()) {
            return null;
        }

        RemappedExpressions output = remapExpressions(projection(outputCalc), condition(outputCalc), timeIndex);
        if (output == null) {
            return null;
        }
        int[] partition = originalSpec.getPartition().getFieldIndices().clone();
        for (int index = 0; index < partition.length; index++) {
            if (partition[index] == timeIndex) {
                return null;
            }
            if (partition[index] > timeIndex) {
                partition[index]--;
            }
        }
        List<org.apache.calcite.rel.core.AggregateCall> calls =
                new ArrayList<>(originalGroup.getAggCalls().size());
        for (org.apache.calcite.rel.core.AggregateCall call : originalGroup.getAggCalls()) {
            List<Integer> arguments = new ArrayList<>(call.getArgList().size());
            for (int argument : call.getArgList()) {
                if (argument == timeIndex) {
                    return null;
                }
                arguments.add(argument > timeIndex ? argument - 1 : argument);
            }
            org.apache.calcite.rel.core.AggregateCall remapped = call.withArgList(arguments);
            if (call.filterArg == timeIndex) {
                return null;
            }
            if (call.filterArg > timeIndex) {
                remapped = remapped.withFilter(call.filterArg - 1);
            }
            calls.add(remapped);
        }
        SortSpec sort = SortSpec.builder()
                .addField(
                        0,
                        originalGroup.getSort().getAscendingOrders()[0],
                        originalGroup.getSort().getNullsIsLast()[0])
                .build();
        RexWindowBound lowerBound = remapBound(originalGroup.getLowerBound(), timeIndex);
        RexWindowBound upperBound = remapBound(originalGroup.getUpperBound(), timeIndex);
        if (lowerBound == null || upperBound == null) {
            return null;
        }
        OverSpec remappedSpec = new OverSpec(
                new PartitionSpec(partition),
                List.of(new OverSpec.GroupSpec(sort, originalGroup.isRows(), lowerBound, upperBound, calls)),
                originalSpec.getConstants(),
                originalSpec.getOriginalInputFields() - 1);
        RowType inputType = withoutField((RowType) inputCalc.getOutputType(), timeIndex);
        RowType overOutputType = withoutField((RowType) aggregate.getOutputType(), timeIndex);
        return new ProcessingTimeOverAggregate(
                aggregate,
                inputCalc,
                inputCalc.getInputEdges().get(0),
                inputProjection,
                inputType,
                remappedSpec,
                overOutputType,
                outputCalc,
                output.projection,
                output.condition);
    }

    /**
     * Removes Flink's synthetic {@code PROCTIME()} field when it is consumed only as the ordering
     * marker of a processing-time deduplicate. The native operator uses arrival order directly;
     * observable processing-time values remain on Flink.
     */
    private static ProcessingTimeDeduplicate processingTimeDeduplicate(StreamExecCalc outputCalc) {
        if (outputCalc.getInputEdges().size() != 1
                || !(outputCalc.getInputEdges().get(0).getSource() instanceof StreamExecDeduplicate)) {
            return null;
        }
        StreamExecDeduplicate deduplicate =
                (StreamExecDeduplicate) outputCalc.getInputEdges().get(0).getSource();
        if (booleanField(deduplicate, "isRowtime")
                || deduplicate.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge deduplicateInput = deduplicate.getInputEdges().get(0);
        if (!(deduplicateInput.getSource() instanceof StreamExecExchange)) {
            return null;
        }
        StreamExecExchange exchange = (StreamExecExchange) deduplicateInput.getSource();
        if (exchange.getInputEdges().size() != 1
                || !(exchange.getInputEdges().get(0).getSource() instanceof StreamExecCalc)) {
            return null;
        }
        StreamExecCalc inputCalc =
                (StreamExecCalc) exchange.getInputEdges().get(0).getSource();
        if (inputCalc.getInputEdges().size() != 1) {
            return null;
        }
        List<RexNode> originalProjection = projection(inputCalc);
        int timeIndex = -1;
        for (int index = 0; index < originalProjection.size(); index++) {
            if (isProctimeCall(originalProjection.get(index))) {
                if (timeIndex >= 0) {
                    return null;
                }
                timeIndex = index;
            }
        }
        if (timeIndex < 0) {
            return null;
        }
        int[] keys = uniqueKeys(deduplicate);
        for (int index = 0; index < keys.length; index++) {
            if (keys[index] == timeIndex) {
                return null;
            }
            if (keys[index] > timeIndex) {
                keys[index]--;
            }
        }
        RemappedExpressions output = remapExpressions(projection(outputCalc), condition(outputCalc), timeIndex);
        if (output == null) {
            return null;
        }
        List<RexNode> inputProjection = new ArrayList<>(originalProjection);
        inputProjection.remove(timeIndex);
        if (inputProjection.isEmpty()) {
            return null;
        }
        return new ProcessingTimeDeduplicate(
                deduplicate,
                exchange,
                inputCalc,
                inputCalc.getInputEdges().get(0),
                inputProjection,
                withoutField((RowType) inputCalc.getOutputType(), timeIndex),
                keys,
                withoutField((RowType) deduplicate.getOutputType(), timeIndex),
                outputCalc,
                output.projection,
                output.condition);
    }

    private static RowType withoutField(RowType type, int removed) {
        List<RowType.RowField> fields = new ArrayList<>(type.getFields());
        fields.remove(removed);
        return new RowType(type.isNullable(), fields);
    }

    private static RexWindowBound remapBound(RexWindowBound bound, int removed) {
        if (bound.isCurrentRow() || bound.isUnbounded()) {
            return bound;
        }
        RexNode offset = bound.getOffset();
        if (!(offset instanceof RexInputRef)) {
            return null;
        }
        RexInputRef inputRef = (RexInputRef) offset;
        if (inputRef.getIndex() == removed) {
            return null;
        }
        int index = inputRef.getIndex() > removed ? inputRef.getIndex() - 1 : inputRef.getIndex();
        RexInputRef remapped = new RexInputRef(index, inputRef.getType());
        return bound.isPreceding() ? RexWindowBounds.preceding(remapped) : RexWindowBounds.following(remapped);
    }

    private static RemappedExpressions remapExpressions(List<RexNode> projection, RexNode condition, int removed) {
        boolean[] observedRemovedField = {false};
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                if (inputRef.getIndex() == removed) {
                    observedRemovedField[0] = true;
                    return inputRef;
                }
                int index = inputRef.getIndex() > removed ? inputRef.getIndex() - 1 : inputRef.getIndex();
                return index == inputRef.getIndex() ? inputRef : new RexInputRef(index, inputRef.getType());
            }
        };
        List<RexNode> remappedProjection = projection.stream()
                .map(expression -> expression.accept(shuttle))
                .collect(Collectors.toList());
        RexNode remappedCondition = condition == null ? null : condition.accept(shuttle);
        return observedRemovedField[0] ? null : new RemappedExpressions(remappedProjection, remappedCondition);
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

    private static LegacyGroupWindowAggregate legacyGroupWindowAggregate(StreamExecGroupWindowAggregate node) {
        int[] grouping = ((int[]) field(node, StreamExecGroupWindowAggregate.class, "grouping")).clone();
        org.apache.calcite.rel.core.AggregateCall[] aggregateCalls = ((org.apache.calcite.rel.core.AggregateCall[])
                        field(node, StreamExecGroupWindowAggregate.class, "aggCalls"))
                .clone();
        LogicalWindow logicalWindow = (LogicalWindow) field(node, StreamExecGroupWindowAggregate.class, "window");
        NamedWindowProperty[] properties = ((NamedWindowProperty[])
                        field(node, StreamExecGroupWindowAggregate.class, "namedWindowProperties"))
                .clone();
        boolean needRetraction = (boolean) field(node, StreamExecGroupWindowAggregate.class, "needRetraction");
        return new LegacyGroupWindowAggregate(
                node, grouping, aggregateCalls, logicalWindow, properties, needRetraction);
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

    private static final class LegacyGroupWindowAggregate {
        private final StreamExecGroupWindowAggregate node;
        private final int[] grouping;
        private final org.apache.calcite.rel.core.AggregateCall[] aggregateCalls;
        private final LogicalWindow window;
        private final NamedWindowProperty[] properties;
        private final boolean needRetraction;

        private LegacyGroupWindowAggregate(
                StreamExecGroupWindowAggregate node,
                int[] grouping,
                org.apache.calcite.rel.core.AggregateCall[] aggregateCalls,
                LogicalWindow window,
                NamedWindowProperty[] properties,
                boolean needRetraction) {
            this.node = node;
            this.grouping = grouping;
            this.aggregateCalls = aggregateCalls;
            this.window = window;
            this.properties = properties;
            this.needRetraction = needRetraction;
        }
    }

    private static final class TwoPhaseGroupAggregate {
        private final StreamExecGlobalGroupAggregate global;
        private final StreamExecExchange exchange;
        private final StreamExecLocalGroupAggregate local;
        private final ExecEdge inputEdge;

        private TwoPhaseGroupAggregate(
                StreamExecGlobalGroupAggregate global,
                StreamExecExchange exchange,
                StreamExecLocalGroupAggregate local,
                ExecEdge inputEdge) {
            this.global = global;
            this.exchange = exchange;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }

    private static final class IncrementalGroupAggregate {
        private final StreamExecGlobalGroupAggregate global;
        private final StreamExecExchange finalExchange;
        private final StreamExecIncrementalGroupAggregate incremental;
        private final StreamExecExchange partialExchange;
        private final StreamExecLocalGroupAggregate local;
        private final ExecEdge inputEdge;

        private IncrementalGroupAggregate(
                StreamExecGlobalGroupAggregate global,
                StreamExecExchange finalExchange,
                StreamExecIncrementalGroupAggregate incremental,
                StreamExecExchange partialExchange,
                StreamExecLocalGroupAggregate local,
                ExecEdge inputEdge) {
            this.global = global;
            this.finalExchange = finalExchange;
            this.incremental = incremental;
            this.partialExchange = partialExchange;
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

    private static final class RemappedExpressions {
        private final List<RexNode> projection;
        private final RexNode condition;

        private RemappedExpressions(List<RexNode> projection, RexNode condition) {
            this.projection = projection;
            this.condition = condition;
        }
    }

    private static final class ProcessingTimeOverAggregate {
        private final StreamExecOverAggregate aggregate;
        private final StreamExecCalc inputCalc;
        private final ExecEdge inputEdge;
        private final List<RexNode> inputProjection;
        private final RowType inputType;
        private final OverSpec overSpec;
        private final RowType overOutputType;
        private final StreamExecCalc outputCalc;
        private final List<RexNode> outputProjection;
        private final RexNode outputCondition;

        private ProcessingTimeOverAggregate(
                StreamExecOverAggregate aggregate,
                StreamExecCalc inputCalc,
                ExecEdge inputEdge,
                List<RexNode> inputProjection,
                RowType inputType,
                OverSpec overSpec,
                RowType overOutputType,
                StreamExecCalc outputCalc,
                List<RexNode> outputProjection,
                RexNode outputCondition) {
            this.aggregate = aggregate;
            this.inputCalc = inputCalc;
            this.inputEdge = inputEdge;
            this.inputProjection = inputProjection;
            this.inputType = inputType;
            this.overSpec = overSpec;
            this.overOutputType = overOutputType;
            this.outputCalc = outputCalc;
            this.outputProjection = outputProjection;
            this.outputCondition = outputCondition;
        }
    }

    private static final class ProcessingTimeDeduplicate {
        private final StreamExecDeduplicate deduplicate;
        private final StreamExecExchange exchange;
        private final StreamExecCalc inputCalc;
        private final ExecEdge inputEdge;
        private final List<RexNode> inputProjection;
        private final RowType inputType;
        private final int[] uniqueKeys;
        private final RowType deduplicateOutputType;
        private final StreamExecCalc outputCalc;
        private final List<RexNode> outputProjection;
        private final RexNode outputCondition;

        private ProcessingTimeDeduplicate(
                StreamExecDeduplicate deduplicate,
                StreamExecExchange exchange,
                StreamExecCalc inputCalc,
                ExecEdge inputEdge,
                List<RexNode> inputProjection,
                RowType inputType,
                int[] uniqueKeys,
                RowType deduplicateOutputType,
                StreamExecCalc outputCalc,
                List<RexNode> outputProjection,
                RexNode outputCondition) {
            this.deduplicate = deduplicate;
            this.exchange = exchange;
            this.inputCalc = inputCalc;
            this.inputEdge = inputEdge;
            this.inputProjection = inputProjection;
            this.inputType = inputType;
            this.uniqueKeys = uniqueKeys;
            this.deduplicateOutputType = deduplicateOutputType;
            this.outputCalc = outputCalc;
            this.outputProjection = outputProjection;
            this.outputCondition = outputCondition;
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
