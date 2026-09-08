/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;

/** Records the original and selected graphs while requiring ordinary whole-plan admission. */
public final class SelectedLocalWindowSqlProbe implements ExecNodeGraphProcessor {
    static List<ExecNode<?>> originals;
    static List<ExecNode<?>> selected;

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        originals = nodes(graph.getRootNodes());
        var processor = new StreamFusionExecGraphProcessor();
        var ordinary = processor.process(graph, context);
        if (ordinary == graph)
            throw new AssertionError(
                    "Window production admission failed: " + StreamFusionPlanningDiagnostics.explain());
        selected = nodes(ordinary.getRootNodes());
        return ordinary;
    }

    private static List<ExecNode<?>> nodes(List<ExecNode<?>> roots) {
        var result = new ArrayList<ExecNode<?>>();
        var pending = new ArrayList<>(roots);
        var seen = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
        while (!pending.isEmpty()) {
            var node = pending.remove(pending.size() - 1);
            if (!seen.add(node)) continue;
            result.add(node);
            for (var edge : node.getInputEdges()) pending.add(edge.getSource());
        }
        return result;
    }
}
