/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;

/** Immutable ownership and port layout for connected native DAGs between Flink boundaries. */
final class StreamFusionNativeRegionLayout {
    final List<Region> regions;
    private final Map<ExecNode<?>, Region> owners;
    private final Map<ExecNode<?>, List<String>> shared;
    private final Map<ExecNode<?>, List<ExecNode<?>>> boundaryInputs;

    private StreamFusionNativeRegionLayout(Builder builder) {
        Map<ExecNode<?>, List<ExecNode<?>>> groups = new IdentityHashMap<>();
        var orderedGroups = new ArrayList<List<ExecNode<?>>>();
        for (ExecNode<?> node : builder.ordered) {
            if (builder.nativeNodes.containsKey(node)) {
                var group = groups.get(builder.owner(node));
                if (group == null) {
                    group = new ArrayList<>();
                    groups.put(builder.owner(node), group);
                    orderedGroups.add(group);
                }
                group.add(node);
            }
        }
        var result = new ArrayList<Region>();
        owners = new IdentityHashMap<>();
        shared = new IdentityHashMap<>();
        boundaryInputs = new IdentityHashMap<>(builder.inputs);
        for (var stages : orderedGroups) {
            var region = new Region(stages, builder.uses, builder.inputs);
            result.add(region);
            for (var stage : stages) {
                owners.put(stage, region);
                var consumers = builder.uses.get(stage);
                if (consumers.size() > 1
                        && consumers.stream().anyMatch(use -> builder.nativeNodes.containsKey(use.consumer)))
                    shared.put(
                            stage,
                            consumers.stream().map(Use::description).collect(java.util.stream.Collectors.toList()));
            }
        }
        regions = List.copyOf(result);
    }

    static StreamFusionNativeRegionLayout discover(ExecNodeGraph graph, Predicate<ExecNode<?>> nativeNode) {
        var builder = new Builder(nativeNode);
        for (int index = 0; index < graph.getRootNodes().size(); index++) {
            var root = graph.getRootNodes().get(index);
            builder.uses.computeIfAbsent(root, ignored -> new ArrayList<>()).add(new Use(null, index));
            builder.visit(root);
        }
        for (var node : builder.ordered) {
            if (!builder.nativeNodes.containsKey(node)) continue;
            for (var child : builder.inputs.get(node)) {
                if (builder.nativeNodes.containsKey(child)
                        && (node instanceof BatchExecNode) == (child instanceof BatchExecNode))
                    builder.nativeNodes.put(builder.owner(node), builder.owner(child));
            }
        }
        return new StreamFusionNativeRegionLayout(builder);
    }

    Region owner(ExecNode<?> node) {
        return owners.get(node);
    }

    Map<ExecNode<?>, List<String>> sharedInternalStages() {
        return Collections.unmodifiableMap(shared);
    }

    /** Contracting a valid SQL DAG must not create a task that consumes its own exchange output. */
    void validateBoundaryGraph() {
        Map<Object, Boolean> visited = new IdentityHashMap<>();
        for (var region : regions) visitBoundary(region, visited);
    }

    private void visitBoundary(Object value, Map<Object, Boolean> visited) {
        var complete = visited.putIfAbsent(value, false);
        if (Boolean.FALSE.equals(complete))
            throw new IllegalArgumentException("Fusing native stages would create a cycle across a Flink boundary");
        if (complete != null) return;
        List<ExecNode<?>> dependencies;
        if (value instanceof Region) dependencies = ((Region) value).inputs;
        else dependencies = boundaryInputs.get((ExecNode<?>) value);
        for (var node : dependencies) {
            var region = owners.get(node);
            visitBoundary(region == null ? node : region, visited);
        }
        visited.put(value, true);
    }

    /** Each physical stage is defined once. References preserve input order and repeated uses. */
    static final class Region {
        final List<ExecNode<?>> stages;
        final List<ExecNode<?>> inputs;
        final List<ExecNode<?>> outputs;
        final List<List<Reference>> stageInputs;

        private Region(
                List<ExecNode<?>> stages,
                Map<ExecNode<?>, List<Use>> uses,
                Map<ExecNode<?>, List<ExecNode<?>>> originalInputs) {
            this.stages = List.copyOf(stages);
            Map<ExecNode<?>, Integer> indices = new IdentityHashMap<>();
            for (int index = 0; index < stages.size(); index++) indices.put(stages.get(index), index);
            var inputList = new ArrayList<ExecNode<?>>();
            var outputList = new ArrayList<ExecNode<?>>();
            var bindings = new ArrayList<List<Reference>>();
            for (var stage : stages) {
                var references = new ArrayList<Reference>();
                for (var source : originalInputs.get(stage)) {
                    Integer internal = indices.get(source);
                    if (internal != null) references.add(new Reference(false, internal));
                    else {
                        // External edges are Flink input channels. Even the same boundary
                        // source can supply distinct channels with independent barrier state.
                        references.add(new Reference(true, inputList.size()));
                        inputList.add(source);
                    }
                }
                bindings.add(List.copyOf(references));
                if (uses.get(stage).stream().anyMatch(use -> !indices.containsKey(use.consumer))) outputList.add(stage);
            }
            inputs = List.copyOf(inputList);
            outputs = List.copyOf(outputList);
            stageInputs = List.copyOf(bindings);
        }
    }

    static final class Reference {
        final boolean external;
        final int index;

        private Reference(boolean external, int index) {
            this.external = external;
            this.index = index;
        }
    }

    private static final class Use {
        final ExecNode<?> consumer;
        final int port;

        Use(ExecNode<?> consumer, int port) {
            this.consumer = consumer;
            this.port = port;
        }

        String description() {
            return consumer == null
                    ? "root[" + port + "]"
                    : consumer.getClass().getSimpleName() + "#" + consumer.getId() + "/input[" + port + "]";
        }
    }

    private static final class Builder {
        final Predicate<ExecNode<?>> nativeNode;
        final List<ExecNode<?>> ordered = new ArrayList<>();
        final Map<ExecNode<?>, Boolean> visited = new IdentityHashMap<>();
        final Map<ExecNode<?>, ExecNode<?>> nativeNodes = new IdentityHashMap<>();
        final Map<ExecNode<?>, List<Use>> uses = new IdentityHashMap<>();
        final Map<ExecNode<?>, List<ExecNode<?>>> inputs = new IdentityHashMap<>();

        Builder(Predicate<ExecNode<?>> nativeNode) {
            this.nativeNode = nativeNode;
        }

        void visit(ExecNode<?> node) {
            var complete = visited.putIfAbsent(node, false);
            if (Boolean.FALSE.equals(complete))
                throw new IllegalArgumentException("Native region graph contains a physical-plan cycle");
            if (complete != null) return;
            if (nativeNode.test(node)) nativeNodes.put(node, node);
            var sources = node.getInputEdges().stream()
                    .map(edge -> edge.getSource())
                    .collect(java.util.stream.Collectors.toList());
            inputs.put(node, List.copyOf(sources));
            for (int index = 0; index < sources.size(); index++) {
                var source = sources.get(index);
                uses.computeIfAbsent(source, ignored -> new ArrayList<>()).add(new Use(node, index));
                visit(source);
            }
            visited.put(node, true);
            ordered.add(node);
        }

        ExecNode<?> owner(ExecNode<?> node) {
            var parent = nativeNodes.get(node);
            if (parent != node) nativeNodes.put(node, parent = owner(parent));
            return parent;
        }
    }
}
