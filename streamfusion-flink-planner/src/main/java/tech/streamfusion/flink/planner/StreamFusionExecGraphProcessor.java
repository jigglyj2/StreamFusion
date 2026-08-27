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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.types.logical.RowType;

/** All-or-nothing physical rule modelled after Comet's distinct accelerator exec nodes. */
public final class StreamFusionExecGraphProcessor implements ExecNodeGraphProcessor {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.calc.StreamFusionCalcTranslator";

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        if (!graph.getRootNodes().stream().allMatch(node -> isEligible(node, context))) {
            return graph;
        }
        List<ExecNode<?>> roots =
                graph.getRootNodes().stream().map(this::convert).collect(Collectors.toList());
        return new ExecNodeGraph(graph.getFlinkVersion(), roots);
    }

    private boolean isEligible(ExecNode<?> node, ProcessorContext context) {
        if (node.getInputEdges().isEmpty()) {
            return true;
        }
        if (node instanceof StreamExecCalc) {
            return canTranslate((StreamExecCalc) node, context)
                    && node.getInputEdges().stream().allMatch(edge -> isEligible(edge.getSource(), context));
        }
        if (node.getClass().getSimpleName().equals("StreamExecSink")) {
            return node.getInputEdges().stream().allMatch(edge -> isEligible(edge.getSource(), context));
        }
        return false;
    }

    private ExecNode<?> convert(ExecNode<?> node) {
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

    private boolean canTranslate(StreamExecCalc calc, ProcessorContext context) {
        ExecEdge input = calc.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method =
                    translator.getMethod("canTranslate", RowType.class, RowType.class, List.class, Object.class);
            return (boolean) method.invoke(
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

    @SuppressWarnings("unchecked")
    private static List<RexNode> projection(StreamExecCalc calc) {
        return (List<RexNode>) field(calc, "projection");
    }

    private static RexNode condition(StreamExecCalc calc) {
        return (RexNode) field(calc, "condition");
    }

    private static Object field(StreamExecCalc calc, String name) {
        try {
            Field field = CommonExecCalc.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(calc);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Could not read Flink calc " + name, e);
        }
    }
}
