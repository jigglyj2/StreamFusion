/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;

/** Binds copies in the generated graph, preserving reusable transformations across executions. */
public final class NativePipelineResourceFinalizer {
    private NativePipelineResourceFinalizer() {}

    public static void finalizePipeline(StreamGraph graph, List<Transformation<?>> pipelineRoots) {
        var resolved = new IdentityHashMap<
                Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>>,
                Map<Long, FlinkOperatorMemoryShare>>();
        var replacements = new IdentityHashMap<StreamNode, StreamFusionNativeRegionOperatorFactory>();
        for (var node : graph.getStreamNodes()) {
            if (!(node.getOperatorFactory() instanceof StreamFusionNativeRegionOperatorFactory)) continue;
            var factory = (StreamFusionNativeRegionOperatorFactory) node.getOperatorFactory();
            var resolver = factory.resourceResolver();
            if (resolver == null) continue;
            var shares = resolved.computeIfAbsent(
                    resolver, callback -> Map.copyOf(callback.apply(List.copyOf(pipelineRoots))));
            replacements.put(node, factory.withResolvedResources(shares));
        }
        // All owners must resolve before publishing any changed factory into the graph.
        replacements.forEach(StreamNode::setOperatorFactory);
    }
}
