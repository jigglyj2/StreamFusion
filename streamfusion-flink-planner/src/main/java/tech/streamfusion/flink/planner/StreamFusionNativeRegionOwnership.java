/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

/** Restricts reused execution to the verified divergent streaming region shape. */
final class StreamFusionNativeRegionOwnership {
    private StreamFusionNativeRegionOwnership() {}

    static Map<ExecNode<?>, List<String>> unsupportedSharedStages(
            ExecNodeGraph graph, Predicate<ExecNode<?>> nativeNode) {
        var layout = StreamFusionNativeRegionLayout.discover(graph, nativeNode);
        layout.validateBoundaryGraph();
        var unsupported = new java.util.IdentityHashMap<ExecNode<?>, List<String>>();
        for (var entry : layout.sharedInternalStages().entrySet()) {
            var owner = layout.owner(entry.getKey());
            boolean supported = owner.outputs.size() > 1
                    && owner.inputs.size() == 1
                    && owner.stages.stream()
                            .allMatch(stage -> stage
                                            instanceof
                                            org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode
                                    && stage.getInputEdges().size() == 1);
            // Typed protobuf preflight additionally verifies matching window-clock frontiers
            // and disabled sampled latency before the selected graph is committed.
            if (!supported) unsupported.put(entry.getKey(), entry.getValue());
        }
        return unsupported;
    }
}
