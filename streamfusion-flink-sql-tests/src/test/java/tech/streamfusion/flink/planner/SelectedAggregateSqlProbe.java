/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecValues;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;

/** Test-only probe of real SQL graph conversion, never a production admission override. */
public final class SelectedAggregateSqlProbe implements ExecNodeGraphProcessor {
    public static int convertedGraphs;
    public static boolean recordOnly;
    public static String inputGraph;
    private static List<ExecNode<?>> convertedRoots = List.of();
    private static java.util.Set<Integer> originalStageIds = java.util.Set.of();

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        var descriptions = new ArrayList<String>();
        originalStageIds = new java.util.HashSet<>();
        for (var root : graph.getRootNodes()) describe(root, descriptions);
        inputGraph = descriptions.toString();
        if (recordOnly) return graph;
        var processor = new StreamFusionExecGraphProcessor();
        var result = processor.process(graph, context);
        if (result != graph) {
            convertedRoots = List.copyOf(result.getRootNodes());
            convertedGraphs++;
            return result;
        }
        var report = StreamFusionPlanningDiagnostics.explain();
        var reasons =
                report.lines().filter(line -> line.startsWith("Fallback:")).toArray(String[]::new);
        if (reasons.length == 0) throw new AssertionError("No admission evidence: " + report);
        for (String reason : reasons)
            if (!reason.contains(": architecture:"))
                throw new AssertionError("SQL semantic preflight failed: " + report);
        var roots = new ArrayList<ExecNode<?>>();
        for (var root : graph.getRootNodes()) roots.add(processor.convert(root));
        convertedRoots = roots;
        convertedGraphs++;
        return new ExecNodeGraph(graph.getFlinkVersion(), roots);
    }

    private static void describe(ExecNode<?> node, List<String> descriptions) {
        originalStageIds.add(node.getId());
        descriptions.add(node.getDescription().replaceAll("\\*anonymous_[^*]+\\*", "*anonymous*"));
        if (node instanceof CommonExecValues)
            descriptions.add(((CommonExecValues) node).getTuples().toString());
        for (var edge : node.getInputEdges()) describe(edge.getSource(), descriptions);
    }

    public static void verifyTranslatedArrowTopology() {
        int owners = 0;
        for (var root : convertedRoots) {
            verifyNoInternalJvmOperators(root);
            var transformation = ((ExecNodeBase<?>) root).getTransformation();
            if (transformation == null) throw new AssertionError("SQL root was never translated");
            for (var stage : transformation.getTransitivePredecessors()) {
                if (stage instanceof KeyedMultipleInputTransformation) {
                    var keyed = (KeyedMultipleInputTransformation<?>) stage;
                    if (!(keyed.getOperatorFactory() instanceof StreamFusionNativeRegionOperatorFactory))
                        throw new AssertionError("Unexpected keyed runtime " + keyed.getOperatorFactory());
                    if (keyed.getOutputType() != ArrowRowDataBatchTypeInfo.INSTANCE)
                        throw new AssertionError("Native SQL region output is not Arrow");
                    for (var input : keyed.getInputs())
                        if (!(input.getOutputType() instanceof NativeExchangeFrameTypeInfo))
                            throw new AssertionError("Native keyed SQL edge is not Arrow IPC");
                    owners++;
                }
                if (stage instanceof OneInputTransformation) {
                    var unary = (OneInputTransformation<?, ?>) stage;
                    if (unary.getOperatorFactory() instanceof SimpleOperatorFactory) {
                        var operator = unary.getOperator();
                        if (operator.getClass().getName().startsWith("tech.streamfusion.")
                                && !operator.getClass()
                                        .getName()
                                        .equals("tech.streamfusion.flink.arrow.ArrowBatchToRowDataOperator")
                                && unary.getOutputType() != ArrowRowDataBatchTypeInfo.INSTANCE
                                && !(unary.getOutputType() instanceof NativeExchangeFrameTypeInfo))
                            throw new AssertionError("Internal RowData-shaped native stage " + operator.getClass());
                    }
                }
            }
        }
        if (owners != 1) throw new AssertionError("Expected one native SQL state owner, got " + owners);
    }

    // Check the shared fragment contract, not a whitelist of particular operator pairs.
    private static void verifyNoInternalJvmOperators(ExecNode<?> node) {
        if (node instanceof StreamFusionNativePlanNode) {
            var nativeNode = (StreamFusionNativePlanNode) node;
            if (!originalStageIds.contains(nativeNode.nativeMetadata().physicalNodeId(node)))
                throw new AssertionError(
                        "Native stage lost its original physical metric identity: " + node.getDescription());
        }
        for (var edge : node.getInputEdges()) {
            var child = edge.getSource();
            if (node instanceof StreamFusionNativePlanNode
                    && child instanceof StreamFusionNativePlanNode
                    && (node instanceof BatchExecNode) == (child instanceof BatchExecNode)
                    && ((ExecNodeBase<?>) child).getTransformation() != null)
                throw new AssertionError("Internal native stage created a JVM operator: " + child.getDescription());
            verifyNoInternalJvmOperators(child);
        }
    }
}
