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
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExpand;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction;
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
                graph.getRootNodes().stream().map(this::convert).collect(Collectors.toList());
        return new ExecNodeGraph(graph.getFlinkVersion(), roots);
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
        } else if (node instanceof StreamExecDeduplicate) {
            String reason = unsupportedReason((StreamExecDeduplicate) node, context);
            if (reason != null) {
                rejections.add(nodePath + "\n" + reason);
            }
        } else if (node instanceof StreamExecDropUpdateBefore) {
            // RowKind is Flink changelog metadata, so this node is always eligible.
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
                    "unsupportedReason", RowType.class, RowType.class, TimeAttributeWindowingStrategy.class);
            return (String) method.invoke(
                    null, (RowType) input.getOutputType(), (RowType) window.getOutputType(), windowStrategy(window));
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
