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

import java.lang.reflect.InvocationTargetException;
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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort;
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
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.FixedMatchRecognize;
import tech.streamfusion.flink.planner.StreamFusionMatchRecognizePlanner.ProcessingTimeMatchRecognize;

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
            // Inspect the original complete graph, including nodes that semantic lowering folds
            // away. Architecture readiness must not be bypassed by a specialized conversion.
            StreamFusionArchitectureSupport.collect(graph, rejections, activeTableConfig);
            String runtimeRejection = runtimePreflightRejection(context);
            if (runtimeRejection != null) {
                rejections.add("runtime-preflight\n" + runtimeRejection);
            }
            for (int index = 0; index < graph.getRootNodes().size(); index++) {
                if (runtimeRejection == null) {
                    collectRejectionsSafely(
                            graph.getRootNodes().get(index), context, "root[" + index + "]", rejections);
                }
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
        try {
            Class.forName(NATIVE_PLAN_CLASS, true, classLoader);
            Class.forName(NATIVE_OPERATOR_CLASS, true, classLoader);
            Class<?> region = Class.forName(
                    "tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator", true, classLoader);
            region.getMethod("identifyStage", byte[].class, int.class, String.class, String.class);
            region.getMethod("inputPlan", int.class);
            region.getMethod("composeWithInputs", byte[].class, List.class);
            region.getMethod("translateInputs", List.class, List.class, RowType.class, byte[].class);
            region.getMethod(
                    "translateInputsWithResources",
                    List.class,
                    List.class,
                    RowType.class,
                    byte[].class,
                    java.util.function.Function.class);
            region.getMethod(
                    "translateKeyedInputsWithResources",
                    List.class,
                    List.class,
                    RowType.class,
                    byte[].class,
                    List.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    java.util.function.Function.class);
            Class.forName("tech.streamfusion.flink.metrics.StreamFusionNativeMetricTree", true, classLoader)
                    .getMethod(
                            "forRegion",
                            byte[].class,
                            org.apache.flink.runtime.jobgraph.OperatorID.class,
                            org.apache.flink.runtime.metrics.groups.TaskMetricGroup.class,
                            org.apache.flink.configuration.Configuration.class,
                            int.class);
            region.getMethod(
                    "translateKeyedInputs",
                    List.class,
                    List.class,
                    RowType.class,
                    byte[].class,
                    List.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class);
            Class.forName(
                    "tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory", true, classLoader);
            Class.forName(UNION_TRANSLATOR_CLASS, true, classLoader)
                    .getMethod("createStagePlan", RowType.class, int.class);
            Class.forName(GROUP_AGGREGATE_TRANSLATOR_CLASS, true, StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod(
                            "createStagePlan",
                            RowType.class,
                            RowType.class,
                            int[].class,
                            org.apache.calcite.rel.core.AggregateCall[].class,
                            boolean[].class,
                            boolean.class,
                            boolean.class,
                            long.class,
                            org.apache.flink.configuration.ReadableConfig.class);
            Class.forName(
                            "tech.streamfusion.flink.planner.window.StreamFusionGlobalWindowAggregateTranslator",
                            true,
                            StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod(
                            "createStagePlan",
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            org.apache.calcite.rel.core.AggregateCall[].class,
                            org.apache.flink.table.planner.plan.logical.WindowingStrategy.class,
                            org.apache.flink.table.runtime.groupwindow.NamedWindowProperty[].class,
                            boolean.class,
                            org.apache.flink.configuration.ReadableConfig.class);
            Class.forName(
                            "tech.streamfusion.flink.planner.window.StreamFusionSessionWindowAggregateTranslator",
                            true,
                            StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod(
                            "createStagePlan",
                            RowType.class,
                            RowType.class,
                            int[].class,
                            org.apache.calcite.rel.core.AggregateCall[].class,
                            org.apache.flink.table.planner.plan.logical.WindowingStrategy.class,
                            org.apache.flink.table.runtime.groupwindow.NamedWindowProperty[].class,
                            boolean.class,
                            org.apache.flink.configuration.ReadableConfig.class);
            Class.forName(NATIVE_PREFLIGHT_CLASS, true, classLoader)
                    .getMethod("verify")
                    .invoke(null);
            return null;
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException
                | RuntimeException
                | LinkageError failure) {
            return "StreamFusion runtime classes are not consistently visible from Flink's planner classloader: "
                    + failureDescription(failure);
        }
    }

    private void collectRejectionsSafely(
            ExecNode<?> node, ProcessorContext context, String path, List<String> rejections) {
        try {
            collectRejections(node, context, path, rejections);
        } catch (LinkageError failure) {
            rejections.add(path + "\nStreamFusion capability inspection could not load its runtime classes: "
                    + failureDescription(failure));
        } catch (RuntimeException failure) {
            rejections.add(
                    path + "\nStreamFusion capability inspection was inconclusive: " + failureDescription(failure));
        }
    }

    private static String failureDescription(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getName() + (message == null || message.isBlank() ? "" : ": " + message);
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
        } else if (node instanceof BatchExecValues) {
            String reason = unsupportedReason((BatchExecValues) node, context);
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
        } else if (node instanceof BatchExecCalc) {
            String reason = unsupportedReason((BatchExecCalc) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecHashJoin) {
            String reason = unsupportedReason((BatchExecHashJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecAdaptiveJoin) {
            String reason = unsupportedReason((BatchExecAdaptiveJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecSortMergeJoin) {
            String reason = unsupportedReason((BatchExecSortMergeJoin) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecNestedLoopJoin) {
            BatchExecNestedLoopJoin join = (BatchExecNestedLoopJoin) node;
            String reason = unsupportedReason(join, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            for (int index = 0; index < join.getInputEdges().size(); index++) {
                collectRejections(
                        bypassBatchExchange(join.getInputEdges().get(index)).getSource(),
                        context,
                        nodePath + "/native-input[" + index + "]",
                        rejections);
            }
            return;
        } else if (node instanceof BatchExecHashWindowAggregate || node instanceof BatchExecSortWindowAggregate) {
            BatchWindowAggregatePair pair = batchWindowAggregatePair(node);
            if (pair == null) {
                ExecEdge inputEdge = batchOnePhaseWindowInputEdge(node);
                if (inputEdge == null) {
                    rejections.add(nodePath
                            + "\nbounded window aggregate: expected a one-phase final aggregate or "
                            + "LocalWindowAggregate -> Exchange -> final merge WindowAggregate");
                    return;
                }
                String reason = unsupportedBatchOnePhaseWindowReason((ExecNodeBase<?>) node, inputEdge, context);
                if (reason != null) {
                    rejections.add(nodePath + "\n" + reason);
                }
                collectRejections(inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            String reason = unsupportedReason(pair, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(pair.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof BatchExecHashAggregate) {
            BatchGroupAggregatePair pair = batchGroupAggregatePair(node);
            String reason = pair == null
                    ? unsupportedReason((BatchExecHashAggregate) node, context)
                    : unsupportedReason(pair, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (pair != null) {
                collectRejections(pair.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof BatchExecSortAggregate) {
            BatchGroupAggregatePair pair = batchGroupAggregatePair(node);
            if (pair == null) {
                rejections.add(
                        nodePath + "\nbounded sort aggregate: expected a final merge paired with a local aggregate");
                return;
            }
            String reason = unsupportedReason(pair, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            collectRejections(pair.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof BatchExecOverAggregate) {
            BatchExecOverAggregate aggregate = (BatchExecOverAggregate) node;
            BatchExecSort inputSort = boundedOverInputSort(aggregate);
            if (inputSort == null) {
                rejections.add(nodePath + "\nbounded OVER: expected Flink's required BatchExecSort input");
                return;
            }
            String reason = unsupportedReason(aggregate, context);
            if (reason == null) {
                reason = unsupportedReason(inputSort, context);
            }
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            ExecEdge nativeInput = inputSort.getInputEdges().get(0);
            collectRejections(nativeInput.getSource(), context, nodePath + "/native-input", rejections);
            return;
        } else if (node instanceof BatchExecRank) {
            BatchExecRank rank = (BatchExecRank) node;
            BoundedRankPipeline pipeline = boundedRankPipeline(rank);
            BatchExecSort inputSort = pipeline == null ? boundedRankInputSort(rank) : pipeline.inputSort;
            String reason = unsupportedReason(rank, context);
            if (reason == null && inputSort != null) {
                reason = unsupportedReason(inputSort, context);
            }
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
            if (pipeline != null) {
                collectRejections(pipeline.inputEdge.getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
            if (inputSort != null) {
                collectRejections(
                        inputSort.getInputEdges().get(0).getSource(), context, nodePath + "/native-input", rejections);
                return;
            }
        } else if (node instanceof BatchExecLimit) {
            String reason = unsupportedReason((BatchExecLimit) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecSortLimit) {
            String reason = unsupportedReason((BatchExecSortLimit) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecSort) {
            String reason = unsupportedReason((BatchExecSort) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecCorrelate) {
            String reason = unsupportedReason((StreamExecCorrelate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecCorrelate) {
            String reason = unsupportedReason((BatchExecCorrelate) node, context);
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
                        + "GlobalWindowAggregate so the native stages can preserve Flink's partial-merge contract");
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
            // Only the verified shared SESSION path is admitted here. Inspect every original
            // input as well: legacy PROCTIME folding must not hide a rejected Calc clock contract.
            String reason = unsupportedReason((StreamExecWindowAggregate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
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
            StreamExecMultiJoin multiJoin = (StreamExecMultiJoin) node;
            JoinSpec binaryJoin = binaryMultiJoinSpec(multiJoin);
            String reason = binaryJoin == null
                    ? unsupportedReason(multiJoin, context)
                    : unsupportedReason(binaryJoin, multiJoin, context);
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
        } else if (node instanceof StreamExecSort) {
            String reason = unsupportedReason((StreamExecSort) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWatermarkAssigner) {
            // The distinct node retains Flink's generated expression and watermark state machine.
            // Flink's row representation tolerates precision changes; Arrow may change time units.
            if (!node.getInputEdges().get(0).getOutputType().equals(node.getOutputType())) {
                rejections.add(nodePath + "\nArrow watermark assignment requires unchanged input field types; "
                        + "timestamp precision conversion is not supported");
            }
        } else if (node instanceof StreamExecExpand) {
            String reason = unsupportedReason((StreamExecExpand) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecExpand) {
            String reason = unsupportedReason((BatchExecExpand) node, context);
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
        } else if (node instanceof BatchExecExchange) {
            BatchExecExchange exchange = (BatchExecExchange) node;
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
        } else if (node instanceof BatchExecUnion) {
            String reason = unsupportedReason((BatchExecUnion) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecWindowTableFunction) {
            String reason = unsupportedReason((StreamExecWindowTableFunction) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof BatchExecWindowTableFunction) {
            String reason = unsupportedReason((BatchExecWindowTableFunction) node, context);
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
