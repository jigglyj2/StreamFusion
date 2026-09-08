/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;

/** Client-only original resource ownership, shared by every selected region from one SQL graph. */
final class StreamFusionOriginalWindowResources {
    private final StreamFusionOriginalMemoryPlan original;
    private final Map<Transformation<?>, ExecNode<?>> outputs = new IdentityHashMap<>();
    private final Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>> resolver = this::resolve;

    private StreamFusionOriginalWindowResources(List<ExecNode<?>> roots) {
        try {
            StreamGraphGenerator.class.getDeclaredMethod("applyExternalPipelineProcessor", StreamGraph.class);
        } catch (NoSuchMethodException failure) {
            throw new IllegalArgumentException(
                    "Local windows require the complete-pipeline Flink resource hook", failure);
        }
        original = new StreamFusionOriginalMemoryPlan(new ExecNodeGraph(roots));
    }

    static StreamFusionOriginalWindowResources capture(List<ExecNode<?>> roots) {
        var pending = new ArrayList<>(roots);
        var seen = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
        while (!pending.isEmpty()) {
            var node = pending.remove(pending.size() - 1);
            if (!seen.add(node)) continue;
            if (node instanceof StreamExecLocalWindowAggregate) return new StreamFusionOriginalWindowResources(roots);
            for (var edge : node.getInputEdges()) pending.add(edge.getSource());
        }
        return null;
    }

    Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>> resolver() {
        return resolver;
    }

    void recordOutput(ExecNode<?> originalNode, Transformation<?> output) {
        if (originalNode == null)
            throw new IllegalStateException("Selected resource output has no original Flink node");
        var previous = outputs.putIfAbsent(output, originalNode);
        if (previous != null && previous != originalNode)
            throw new IllegalStateException("One selected output cannot represent different original resource nodes");
    }

    private Map<Long, FlinkOperatorMemoryShare> resolve(List<Transformation<?>> roots) {
        var shares = original.resolve(node -> ((ExecNodeBase<?>) node).getTransformation(), roots, outputs);
        var result = new java.util.HashMap<Long, FlinkOperatorMemoryShare>();
        shares.forEach((id, share) -> result.put(
                id,
                new FlinkOperatorMemoryShare(share.operatorWeight, share.groupOperatorWeight, share.groupUseCases)));
        return Map.copyOf(result);
    }
}
