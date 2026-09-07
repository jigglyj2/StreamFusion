/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

/** Whole-graph ownership inspection, independent of individual operator lowering rules. */
final class StreamFusionNativeRegionOwnership {
    private StreamFusionNativeRegionOwnership() {}

    static Map<ExecNode<?>, List<String>> sharedInternalStages(ExecNodeGraph graph, Predicate<ExecNode<?>> nativeNode) {
        Map<ExecNode<?>, List<Use>> uses = new IdentityHashMap<>();
        Set<ExecNode<?>> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int index = 0; index < graph.getRootNodes().size(); index++) {
            ExecNode<?> root = graph.getRootNodes().get(index);
            uses.computeIfAbsent(root, ignored -> new ArrayList<>()).add(new Use("root[" + index + "]", false));
            inspect(root, nativeNode, visited, uses);
        }
        Map<ExecNode<?>, List<String>> shared = new IdentityHashMap<>();
        uses.forEach((node, consumers) -> {
            // Multiple edge/sink consumers of the same region output are fine: Flink caches
            // that transformation. An internal consumer would instead recursively inline it.
            if (nativeNode.test(node)
                    && consumers.size() > 1
                    && consumers.stream().anyMatch(consumer -> consumer.nativeConsumer)) {
                shared.put(
                        node,
                        consumers.stream()
                                .map(consumer -> consumer.description)
                                .collect(java.util.stream.Collectors.toList()));
            }
        });
        return shared;
    }

    private static void inspect(
            ExecNode<?> node,
            Predicate<ExecNode<?>> nativeNode,
            Set<ExecNode<?>> visited,
            Map<ExecNode<?>, List<Use>> uses) {
        if (!visited.add(node)) {
            return;
        }
        for (int index = 0; index < node.getInputEdges().size(); index++) {
            ExecNode<?> source = node.getInputEdges().get(index).getSource();
            uses.computeIfAbsent(source, ignored -> new ArrayList<>())
                    .add(new Use(
                            node.getClass().getSimpleName() + "#" + node.getId() + "/input[" + index + "]",
                            nativeNode.test(node)));
            inspect(source, nativeNode, visited, uses);
        }
    }

    private static final class Use {
        private final String description;
        private final boolean nativeConsumer;

        private Use(String description, boolean nativeConsumer) {
            this.description = description;
            this.nativeConsumer = nativeConsumer;
        }
    }
}
