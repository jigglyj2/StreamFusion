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
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.types.logical.RowType;

/** All-or-nothing physical rule modelled after Comet's distinct accelerator exec nodes. */
public final class StreamFusionExecGraphProcessor implements ExecNodeGraphProcessor {

    private transient ReadableConfig activeTableConfig;
    private transient StreamFusionGraphRewrite graphRewrite;

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        StreamFusionPlanningDiagnostics.begin();
        // Focused graph-shape tests construct the processor without a planner. Persisted node
        // configuration remains sufficient unless a translated mini-batch node omitted its size.
        activeTableConfig = context == null ? null : context.getPlanner().getTableConfig();
        try {
            List<String> rejections = new ArrayList<>();
            var inspection = new StreamFusionCapabilityInspection(context, rejections);
            // Inspect the original complete graph, including nodes that semantic lowering folds
            // away. Architecture readiness must not be bypassed by a specialized conversion.
            StreamFusionArchitectureSupport.collect(graph, rejections, activeTableConfig);
            String runtimeRejection = runtimePreflightRejection(context);
            if (runtimeRejection != null) {
                rejections.add("runtime-preflight\n" + runtimeRejection);
            }
            for (int index = 0; index < graph.getRootNodes().size(); index++) {
                inspection.collect(graph.getRootNodes().get(index), "root[" + index + "]");
            }
            if (!rejections.isEmpty()) {
                rejections.forEach(rejection -> {
                    int separator = rejection.indexOf('\n');
                    StreamFusionPlanningDiagnostics.reject(
                            rejection.substring(0, separator), rejection.substring(separator + 1));
                });
                return graph;
            }
            try {
                graphRewrite = new StreamFusionGraphRewrite(activeTableConfig, graph.getRootNodes());
                List<ExecNode<?>> roots =
                        graph.getRootNodes().stream().map(this::convertRoot).collect(Collectors.toList());
                if (context != null) {
                    var stateRejections = graphRewrite.stateMetricRejections(
                            config -> StreamFusionStateMetricAdmission.unsupportedReason(
                                    config,
                                    context.getPlanner().getFlinkContext().getClassLoader()));
                    if (!stateRejections.isEmpty()) {
                        for (String rejection : stateRejections) {
                            int separator = rejection.indexOf('\n');
                            StreamFusionPlanningDiagnostics.reject(
                                    rejection.substring(0, separator), rejection.substring(separator + 1));
                        }
                        return graph;
                    }
                }
                var selected = new ExecNodeGraph(graph.getFlinkVersion(), roots);
                graphRewrite.commit(() -> StreamFusionSharedNativeRegion.install(
                        selected, context == null ? null : context.getPlanner()));
                StreamFusionPlanningDiagnostics.accelerate();
                return selected;
            } catch (RuntimeException | LinkageError failure) {
                StreamFusionPlanningDiagnostics.reject(
                        "replacement-preflight",
                        "StreamFusion could not construct the complete replacement graph: "
                                + failureDescription(failure));
                return graph;
            }
        } finally {
            activeTableConfig = null;
            graphRewrite = null;
        }
    }

    private static String runtimePreflightRejection(ProcessorContext context) {
        if (context == null) {
            return null;
        }
        return runtimePreflightRejection(context.getPlanner().getFlinkContext().getClassLoader());
    }

    static String runtimePreflightRejection(ClassLoader classLoader) {
        return StreamFusionRuntimeVisibility.rejection(classLoader);
    }

    private static String failureDescription(Throwable failure) {
        return StreamFusionRuntimeVisibility.failureDescription(failure);
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

    static boolean isSinkBoundary(ExecNode<?> node) {
        String nodeName = node.getClass().getSimpleName();
        return nodeName.equals("StreamExecSink")
                || nodeName.equals("StreamExecLegacySink")
                || nodeName.equals("BatchExecSink")
                || nodeName.equals("BatchExecLegacySink");
    }

    ExecNode<?> convert(ExecNode<?> node) {
        if (graphRewrite != null) {
            return graphRewrite.convert(node, this::convertNode);
        }
        graphRewrite = new StreamFusionGraphRewrite(activeTableConfig, List.of(node));
        try {
            ExecNode<?> result = graphRewrite.convert(node, this::convertNode);
            graphRewrite.commit();
            return result;
        } finally {
            graphRewrite = null;
        }
    }

    private ExecNode<?> convertNode(ExecNode<?> node) {
        if (isSinkBoundary(node)) {
            for (int index = 0; index < node.getInputEdges().size(); index++) {
                ExecEdge edge = node.getInputEdges().get(index);
                ExecNode<?> convertedSource = convert(edge.getSource());
                StreamFusionExecSinkBoundary boundary = new StreamFusionExecSinkBoundary(
                        ((ExecNodeBase<?>) node).getPersistedConfig(),
                        node.getInputProperties().get(index),
                        (RowType) edge.getOutputType());
                boundary.setInputEdges(List.of(copyEdge(edge, convertedSource, boundary)));
                graphRewrite.replaceInputEdge(node, index, copyEdge(edge, boundary, node));
            }
            return node;
        }
        ExecNode<?> replacement = StreamFusionStatelessConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionCalcConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionJoinConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionGroupAggregateConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionOverConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionWindowAggregateConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionRankConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }
        replacement = StreamFusionControlConversions.convert(node, this);
        if (replacement != null) {
            return replacement;
        }

        for (int index = 0; index < node.getInputEdges().size(); index++) {
            ExecEdge edge = node.getInputEdges().get(index);
            graphRewrite.replaceInputEdge(node, index, copyEdge(edge, convert(edge.getSource()), node));
        }
        return node;
    }

    ReadableConfig tableConfig() {
        return activeTableConfig;
    }

    /** Composite semantic rewrites retain every physical stage in the ordinary identity registry. */
    void registerReplacement(ExecNode<?> original, ExecNode<?> replacement) {
        if (graphRewrite == null || graphRewrite.convert(original, ignored -> replacement) != replacement)
            throw new IllegalStateException("A physical stage already has a different replacement");
    }

    ExecNode<?> convertDerived(ExecNode<?> original, java.util.function.Supplier<ExecNode<?>> converter) {
        if (graphRewrite == null) throw new IllegalStateException("Derived stages require an active graph rewrite");
        return graphRewrite.convert(original, ignored -> converter.get());
    }

    long miniBatchSize(ExecNodeBase<?> node) {
        return node.getPersistedConfig()
                .getOptional(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE)
                .orElseGet(() -> activeTableConfig == null
                        ? ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE.defaultValue()
                        : activeTableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE));
    }

    static ExecEdge copyEdge(ExecEdge edge, ExecNode<?> source, ExecNode<?> target) {
        return ExecEdge.builder()
                .source(source)
                .target(target)
                .shuffle(edge.getShuffle())
                .exchangeMode(edge.getExchangeMode())
                .build();
    }

    static ExecEdge bypassBatchExchange(ExecEdge edge) {
        ExecEdge current = edge;
        while (current.getSource() instanceof BatchExecExchange
                && current.getSource().getInputEdges().size() == 1) {
            current = current.getSource().getInputEdges().get(0);
        }
        return current;
    }

    ExecNode<?> convertBatchGroupAggregatePair(BatchGroupAggregatePair pair) {
        int[] grouping = batchGrouping(pair.local);
        org.apache.calcite.rel.core.AggregateCall[] calls = batchAggregateCalls(pair.local);
        RowType originalInputType = (RowType) pair.inputEdge.getOutputType();
        RowType internalType = nativeGroupAccumulatorType(originalInputType, grouping);

        StreamFusionBatchExecLocalGroupAggregate local = new StreamFusionBatchExecLocalGroupAggregate(
                pair.local.getPersistedConfig(),
                grouping,
                calls,
                InputProperty.DEFAULT,
                internalType,
                pair.local instanceof BatchExecHashAggregate);
        local.setInputEdges(List.of(copyEdge(pair.inputEdge, convert(pair.inputEdge.getSource()), local)));

        int[] internalGrouping =
                java.util.stream.IntStream.range(0, grouping.length).toArray();
        InputProperty hashInput = InputProperty.builder()
                .requiredDistribution(
                        grouping.length == 0
                                ? InputProperty.SINGLETON_DISTRIBUTION
                                : InputProperty.hashDistribution(internalGrouping))
                .build();
        StreamFusionBatchExecExchange exchange = new StreamFusionBatchExecExchange(
                pair.global.getPersistedConfig(), hashInput, internalType, "StreamFusionBatchAggregateExchange");
        exchange.setInputEdges(List.of(ExecEdge.builder()
                .source(local)
                .target(exchange)
                .shuffle(ExecEdge.FORWARD_SHUFFLE)
                .build()));

        StreamFusionBatchExecGlobalGroupAggregate global = new StreamFusionBatchExecGlobalGroupAggregate(
                pair.global.getPersistedConfig(),
                originalInputType,
                grouping.length,
                calls,
                hashInput,
                (RowType) pair.global.getOutputType(),
                pair.global instanceof BatchExecHashAggregate);
        global.setInputEdges(List.of(ExecEdge.builder()
                .source(exchange)
                .target(global)
                .shuffle(ExecEdge.FORWARD_SHUFFLE)
                .build()));
        return global;
    }

    ExecNode<?> convertBatchWindowAggregatePair(BatchWindowAggregatePair pair) {
        int[] grouping = batchWindowGrouping(pair.local);
        org.apache.calcite.rel.core.AggregateCall[] calls = batchWindowAggregateCalls(pair.local);
        RowType originalInputType = (RowType) pair.inputEdge.getOutputType();
        RowType internalType = nativeWindowAccumulatorType(originalInputType, grouping);

        StreamFusionBatchExecLocalWindowAggregate local = new StreamFusionBatchExecLocalWindowAggregate(
                pair.local.getPersistedConfig(),
                grouping,
                calls,
                batchWindow(pair.local),
                InputProperty.DEFAULT,
                internalType);
        local.setInputEdges(List.of(copyEdge(pair.inputEdge, convert(pair.inputEdge.getSource()), local)));

        int[] internalGrouping =
                java.util.stream.IntStream.range(0, grouping.length).toArray();
        InputProperty hashInput = InputProperty.builder()
                .requiredDistribution(
                        grouping.length == 0
                                ? InputProperty.SINGLETON_DISTRIBUTION
                                : InputProperty.hashDistribution(internalGrouping))
                .build();
        StreamFusionBatchExecExchange exchange = new StreamFusionBatchExecExchange(
                pair.global.getPersistedConfig(), hashInput, internalType, "StreamFusionBatchWindowAggregateExchange");
        exchange.setInputEdges(List.of(ExecEdge.builder()
                .source(local)
                .target(exchange)
                .shuffle(ExecEdge.FORWARD_SHUFFLE)
                .build()));

        StreamFusionBatchExecGlobalWindowAggregate global = new StreamFusionBatchExecGlobalWindowAggregate(
                pair.global.getPersistedConfig(),
                originalInputType,
                grouping.length,
                calls,
                batchWindow(pair.global),
                batchWindowProperties(pair.global),
                hashInput,
                (RowType) pair.global.getOutputType());
        global.setInputEdges(List.of(ExecEdge.builder()
                .source(exchange)
                .target(global)
                .shuffle(ExecEdge.FORWARD_SHUFFLE)
                .build()));
        return global;
    }

    ExecNode<?> convertBoundedRankPipeline(BoundedRankPipeline pipeline) {
        BatchExecRank rank = pipeline.globalRank;
        StreamFusionBatchExecExchange exchange = new StreamFusionBatchExecExchange(
                pipeline.exchange.getPersistedConfig(),
                pipeline.exchange.getInputProperties().get(0),
                (RowType) pipeline.exchange.getOutputType(),
                "StreamFusionBatchExchange");
        exchange.setInputEdges(
                List.of(copyEdge(pipeline.inputEdge, convert(pipeline.inputEdge.getSource()), exchange)));

        StreamFusionBatchExecRank replacement = new StreamFusionBatchExecRank(
                rank.getPersistedConfig(),
                boundedRankPartitionFields(rank),
                boundedRankSortFields(rank),
                boundedRankStart(rank),
                boundedRankEnd(rank),
                boundedRankOutputNumber(rank),
                boundedSortSpec(pipeline.inputSort),
                rank.getInputProperties().get(0),
                (RowType) rank.getOutputType(),
                "StreamFusionBatchRank");
        replacement.setInputEdges(List.of(copyEdge(rank.getInputEdges().get(0), exchange, replacement)));
        return replacement;
    }
}
