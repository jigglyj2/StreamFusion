/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

/** Admission remains closed until the runtime can execute every exit of the discovered native DAG. */
final class StreamFusionNativeRegionOwnership {
    private StreamFusionNativeRegionOwnership() {}

    static Map<ExecNode<?>, List<String>> sharedInternalStages(ExecNodeGraph graph, Predicate<ExecNode<?>> nativeNode) {
        var layout = StreamFusionNativeRegionLayout.discover(graph, nativeNode);
        layout.validateBoundaryGraph();
        return layout.sharedInternalStages();
    }
}
