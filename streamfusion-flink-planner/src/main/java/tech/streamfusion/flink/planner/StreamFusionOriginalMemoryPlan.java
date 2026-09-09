/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

/** Original physical resource graph; no original internal execution operator is translated. */
final class StreamFusionOriginalMemoryPlan {
    private final List<ExecNode<?>> roots;
    private final Map<ExecNode<?>, Entry> entries = new IdentityHashMap<>();

    StreamFusionOriginalMemoryPlan(ExecNodeGraph graph) {
        roots = List.copyOf(graph.getRootNodes());
        for (var root : roots) capture(root, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    /** Retained boundary transformations must already exist; the callback must not translate nodes. */
    Map<Long, Share> resolve(
            Function<ExecNode<?>, Transformation<?>> translatedBoundary,
            List<Transformation<?>> pipelineRoots,
            Map<Transformation<?>, ExecNode<?>> originalOutputs) {
        var resolver = new Resolver(translatedBoundary);
        for (var root : roots) resolver.node(root);
        var stops = new IdentityHashMap<Transformation<?>, Node>();
        originalOutputs.forEach((output, original) -> {
            if (!entries.containsKey(original)) throw new IllegalArgumentException("Unknown original resource output");
            stops.put(output, resolver.node(original));
        });
        var groups = new HashMap<String, Group>();
        var visited = Collections.newSetFromMap(new IdentityHashMap<Node, Boolean>());
        for (var root : pipelineRoots) sum(resolver.transformation(root, stops), groups, visited);
        var result = new HashMap<Long, Share>();
        entries.forEach((original, entry) -> {
            if (entry.kind != Kind.LOCAL_WINDOW) return;
            var node = resolver.node(original);
            if (!visited.contains(node))
                throw new IllegalArgumentException("Complete pipeline is missing an original local-window output");
            var group = groups.get(node.group);
            long id = (1L << 32) | Integer.toUnsignedLong(original.getId());
            if (result.put(id, new Share(node.weight, group.weight, Set.copyOf(group.useCases))) != null)
                throw new IllegalArgumentException("Original local-window physical identities must be unique");
        });
        return Map.copyOf(result);
    }

    private void capture(ExecNode<?> node, Set<ExecNode<?>> visiting) {
        if (visiting.contains(node)) throw new IllegalArgumentException("Original resource graph contains a cycle");
        if (entries.containsKey(node)) return;
        visiting.add(node);
        var inputs = new ArrayList<ExecNode<?>>();
        for (var edge : node.getInputEdges()) {
            capture(edge.getSource(), visiting);
            inputs.add(edge.getSource());
        }
        entries.put(node, new Entry(kind(node), List.copyOf(inputs)));
        visiting.remove(node);
    }

    private static Kind kind(ExecNode<?> node) {
        switch (node.getClass().getSimpleName()) {
                // ExecNodeUtil rounds the upstream 50/100-byte relative window weights to one MiB unit.
            case "StreamExecLocalWindowAggregate":
                return Kind.LOCAL_WINDOW;
            case "StreamExecWindowAggregate":
            case "StreamExecGlobalWindowAggregate":
                return Kind.GLOBAL_WINDOW;
            case "StreamExecGroupAggregate":
            case "StreamExecGlobalGroupAggregate":
            case "StreamExecJoin":
            case "StreamExecMultiJoin":
            case "StreamExecWindowJoin":
                return Kind.KEYED;
            case "StreamExecCalc":
            case "StreamExecExpand":
            case "StreamExecLocalGroupAggregate":
            case "StreamExecDropUpdateBefore":
            case "StreamExecWatermarkAssigner":
            case "StreamExecMiniBatchAssigner":
            case "StreamExecValues":
                return Kind.STATELESS;
            case "StreamExecUnion":
            case "StreamExecExchange":
                return Kind.VIRTUAL;
            case "StreamExecSink":
            case "StreamExecLegacySink":
                return Kind.BOUNDARY;
            default:
                if (node.getInputEdges().isEmpty()) return Kind.BOUNDARY;
                throw new IllegalArgumentException("Original Flink managed-memory contract is not verified for "
                        + node.getClass().getSimpleName());
        }
    }

    private final class Resolver {
        private final Function<ExecNode<?>, Transformation<?>> translated;
        private final Map<ExecNode<?>, Node> nodes = new IdentityHashMap<>();
        private final Map<Transformation<?>, Node> transformations = new IdentityHashMap<>();
        private final Set<Transformation<?>> visiting = Collections.newSetFromMap(new IdentityHashMap<>());

        private Resolver(Function<ExecNode<?>, Transformation<?>> translated) {
            this.translated = translated;
        }

        private Node node(ExecNode<?> original) {
            var existing = nodes.get(original);
            if (existing != null) return existing;
            var entry = entries.get(original);
            var inputs = new ArrayList<Node>();
            for (var child : entry.inputs) inputs.add(node(child));
            Node result;
            if (entry.kind == Kind.BOUNDARY) {
                // A sink's current edges may point at Arrow adapters after replacement. Stop
                // at those exact transformations and reconnect the saved original inputs.
                if (original.getInputEdges().size() != inputs.size())
                    throw new IllegalArgumentException("Retained boundary resource arity changed");
                var stops = new IdentityHashMap<Transformation<?>, Node>();
                for (int i = 0; i < inputs.size(); i++) {
                    var edge = original.getInputEdges().get(i);
                    stops.put(requireTranslated(edge.getSource()), inputs.get(i));
                }
                result = transformation(requireTranslated(original), stops);
            } else {
                int weight = entry.kind == Kind.LOCAL_WINDOW || entry.kind == Kind.GLOBAL_WINDOW ? 1 : 0;
                var uses = new HashSet<ManagedMemoryUseCase>();
                if (weight > 0) uses.add(ManagedMemoryUseCase.OPERATOR);
                if (entry.kind == Kind.GLOBAL_WINDOW || entry.kind == Kind.KEYED)
                    uses.add(ManagedMemoryUseCase.STATE_BACKEND);
                result = new Node(inputs, null, weight, uses);
            }
            nodes.put(original, result);
            return result;
        }

        private Transformation<?> requireTranslated(ExecNode<?> boundary) {
            var result = translated.apply(boundary);
            if (result == null)
                throw new IllegalArgumentException(
                        "Original resource boundary has not been translated: " + boundary.getDescription());
            return result;
        }

        private Node transformation(Transformation<?> value, Map<Transformation<?>, Node> stops) {
            var stop = stops.get(value);
            if (stop != null) return stop;
            var existing = transformations.get(value);
            if (existing != null) return existing;
            if (!visiting.add(value)) throw new IllegalArgumentException("Boundary resource graph contains a cycle");
            var inputs = new ArrayList<Node>();
            for (var child : value.getInputs()) inputs.add(transformation(child, stops));
            boolean virtual = value instanceof PartitionTransformation
                    || value instanceof UnionTransformation
                    || value instanceof org.apache.flink.streaming.api.transformations.SideOutputTransformation;
            if (!virtual && !(value instanceof SinkTransformation)) {
                String name = value.getClass().getName();
                if (!Set.of(
                                "LegacySourceTransformation",
                                "SourceTransformation",
                                "OneInputTransformation",
                                "TwoInputTransformation",
                                "MultipleInputTransformation",
                                "KeyedMultipleInputTransformation",
                                "LegacySinkTransformation")
                        .stream()
                        .anyMatch(simple -> name.equals("org.apache.flink.streaming.api.transformations." + simple)))
                    throw new IllegalArgumentException(
                            "Original boundary transformation resource contract is not verified: " + name);
            }
            boolean simpleSink = false;
            if (value instanceof SinkTransformation) {
                var sink = ((SinkTransformation<?, ?>) value).getSink();
                if (sink instanceof org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology
                        || sink instanceof org.apache.flink.api.connector.sink2.SupportsCommitter
                        || sink instanceof org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology
                        || sink instanceof org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology)
                    throw new IllegalArgumentException(
                            "Original Flink sink expansion resource contract is not verified: "
                                    + sink.getClass().getName());
                // The standard non-committing Sink V2 expander creates one unkeyed writer;
                // it does not copy managed-memory declarations from SinkTransformation.
                simpleSink = true;
            }
            int weight = virtual || simpleSink
                    ? 0
                    : value.getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
            var uses = new HashSet<ManagedMemoryUseCase>();
            if (!virtual && !simpleSink) {
                uses.addAll(value.getManagedMemoryOperatorScopeUseCaseWeights().keySet());
                uses.addAll(value.getManagedMemorySlotScopeUseCases());
            }
            String specified = virtual
                    ? null
                    : value.getSlotSharingGroup().map(group -> group.getName()).orElse(null);
            var result = new Node(inputs, specified, weight, uses);
            transformations.put(value, result);
            visiting.remove(value);
            return result;
        }
    }

    private static void sum(Node node, Map<String, Group> groups, Set<Node> visited) {
        if (!visited.add(node)) return;
        for (var input : node.inputs) sum(input, groups, visited);
        var group = groups.computeIfAbsent(node.group, ignored -> new Group());
        group.weight = Math.addExact(group.weight, node.weight);
        group.useCases.addAll(node.useCases);
    }

    static final class Share {
        final int operatorWeight;
        final int groupOperatorWeight;
        final Set<ManagedMemoryUseCase> groupUseCases;

        private Share(int operatorWeight, int groupOperatorWeight, Set<ManagedMemoryUseCase> groupUseCases) {
            this.operatorWeight = operatorWeight;
            this.groupOperatorWeight = groupOperatorWeight;
            this.groupUseCases = groupUseCases;
        }
    }

    private enum Kind {
        LOCAL_WINDOW,
        GLOBAL_WINDOW,
        KEYED,
        STATELESS,
        VIRTUAL,
        BOUNDARY
    }

    private static final class Entry {
        final Kind kind;
        final List<ExecNode<?>> inputs;

        Entry(Kind kind, List<ExecNode<?>> inputs) {
            this.kind = kind;
            this.inputs = inputs;
        }
    }

    private static final class Node {
        final List<Node> inputs;
        final String group;
        final int weight;
        final Set<ManagedMemoryUseCase> useCases;

        Node(List<Node> inputs, String specified, int weight, Set<ManagedMemoryUseCase> useCases) {
            this.inputs = List.copyOf(inputs);
            this.weight = weight;
            this.useCases = Set.copyOf(useCases);
            if (specified != null) group = specified;
            else {
                var inherited = new HashSet<String>();
                for (var child : inputs) inherited.add(child.group);
                group = inherited.size() == 1
                        ? inherited.iterator().next()
                        : StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;
            }
        }
    }

    private static final class Group {
        int weight;
        final Set<ManagedMemoryUseCase> useCases = new HashSet<>();
    }
}
