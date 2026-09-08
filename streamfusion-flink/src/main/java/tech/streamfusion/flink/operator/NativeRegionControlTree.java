/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.eventtime.IndexedCombinedWatermarkStatus;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import tech.streamfusion.flink.proto.NativePhysicalPlan;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Flink control propagation through identified native stages, including shared region outputs. */
final class NativeRegionControlTree {
    interface Listener {
        default void inputWatermark(long nodeId, int port, long timestamp) throws Exception {}

        default void endInput(long nodeId) throws Exception {}

        void watermark(long nodeId, long timestamp) throws Exception;

        void status(long nodeId, WatermarkStatus status) throws Exception;

        void latency(long nodeId, LatencyMarker marker) throws Exception;
    }

    private final Edge[] inputs;
    private final Listener listener;
    private final Set<Long> identities = new HashSet<>();
    private final List<Node> outputs;
    private final java.util.Map<Long, Long> restoredWindowWatermarks;

    NativeRegionControlTree(byte[] identifiedPlan, int inputCount, Listener listener) {
        this(identifiedPlan, inputCount, java.util.Map.of(), listener);
    }

    NativeRegionControlTree(
            byte[] identifiedPlan,
            int inputCount,
            java.util.Map<Long, Long> restoredWindowWatermarks,
            Listener listener) {
        this.restoredWindowWatermarks = java.util.Map.copyOf(restoredWindowWatermarks);
        if (inputCount <= 0) {
            throw new IllegalArgumentException("A native control tree needs external inputs");
        }
        inputs = new Edge[inputCount];
        this.listener = java.util.Objects.requireNonNull(listener);
        try {
            NativePlan plan = NativePlan.parseFrom(identifiedPlan);
            if (!plan.hasRoot()) {
                throw new IllegalArgumentException("Native control tree is missing its root");
            }
            outputs = List.of(build(plan.getRoot(), null, 0));
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native control plan", failure);
        }
        validateInputsAndClocks();
    }

    private void validateInputsAndClocks() {
        if (!identities.containsAll(restoredWindowWatermarks.keySet())) {
            throw new IllegalArgumentException("Restored window clock has no matching native stage");
        }
        for (Edge input : inputs) {
            if (input == null) {
                throw new IllegalArgumentException("Native control tree has an unbound external input");
            }
        }
    }

    long rootId() {
        return onlyOutput().id;
    }

    long watermark() {
        return onlyOutput().lastWatermark;
    }

    void watermark(int input, long timestamp) throws Exception {
        var edge = port(input);
        edge.node.watermark(edge.port, timestamp);
    }

    void status(int input, WatermarkStatus status) throws Exception {
        var edge = port(input);
        edge.node.status(edge.port, status);
    }

    void endInput(int input) throws Exception {
        var edge = port(input);
        edge.node.endInput(edge.port);
    }

    boolean contains(long id) {
        return identities.contains(id);
    }

    void latency(int input, LatencyMarker marker) throws Exception {
        latency(port(input).node, marker);
    }

    private void latency(Node node, LatencyMarker marker) throws Exception {
        listener.latency(node.id, marker);
        for (var parent : node.parents) latency(parent.node, marker);
    }

    private Edge port(int input) {
        return inputs[java.util.Objects.checkIndex(input, inputs.length)];
    }

    List<Long> outputIds() {
        return outputs.stream().map(node -> node.id).collect(java.util.stream.Collectors.toList());
    }

    private Node onlyOutput() {
        if (outputs.size() != 1)
            throw new IllegalStateException("Shared native controls require an explicit output port");
        return outputs.get(0);
    }

    NativeRegionControlTree(
            tech.streamfusion.proto.plan.v1.NativeRegionPlan plan,
            java.util.Map<Long, Long> restoredWindowWatermarks,
            Listener listener) {
        NativeRegionPlanComposer.validate(plan);
        this.restoredWindowWatermarks = java.util.Map.copyOf(restoredWindowWatermarks);
        this.listener = java.util.Objects.requireNonNull(listener);
        inputs = new Edge[plan.getInputCount()];
        var nodes = new java.util.LinkedHashMap<Long, Node>();
        for (var stage : plan.getStagesList()) {
            var operator = stage.getOperator();
            register(operator);
            var node = new Node(
                    operator.getPlanNodeId(),
                    null,
                    0,
                    stage.getInputsCount(),
                    operator.hasUnion(),
                    operator.hasWindowAggregate());
            nodes.put(node.id, node);
            for (int port = 0; port < stage.getInputsCount(); port++) {
                var reference = stage.getInputs(port);
                if (reference.hasExternalInput()) inputs[reference.getExternalInput()] = new Edge(node, port);
                else {
                    var child = nodes.get(reference.getStageId());
                    // CommonExecUnion is wiring: nested unions flatten their physical channels.
                    // The region runtime must establish that layout before admitting this subset.
                    if (node.union != null && child.union != null)
                        throw new IllegalArgumentException("Shared native controls require flattened UNION channels");
                    child.parents.add(new Edge(node, port));
                }
            }
        }
        outputs = plan.getOutputStageIdsList().stream().map(nodes::get).collect(java.util.stream.Collectors.toList());
        validateInputsAndClocks();
    }

    private Node build(Operator operator, Node parent, int parentPort) {
        long id = operator.getPlanNodeId();
        register(operator);
        List<Operator> children = NativePhysicalPlan.children(operator);
        if (operator.hasUnion()) {
            List<Operator> channels = new ArrayList<>();
            for (Operator child : children) {
                unionChannels(child, channels);
            }
            children = channels;
        }
        boolean input = operator.getOperatorCase() == Operator.OperatorCase.INPUT;
        if (!input && children.isEmpty()) {
            throw new IllegalArgumentException("Arrival-driven native control stages need physical inputs");
        }
        Node node = new Node(
                id,
                parent,
                parentPort,
                input ? 1 : children.size(),
                operator.hasUnion(),
                operator.hasWindowAggregate());
        if (input) {
            int port = operator.getInput().getInputIndex();
            if (port < 0 || port >= inputs.length || inputs[port] != null) {
                throw new IllegalArgumentException("Native control input slots must be unique and in range");
            }
            inputs[port] = new Edge(node, 0);
        }
        for (int index = 0; index < children.size(); index++) {
            build(children.get(index), node, index);
        }
        return node;
    }

    private void register(Operator operator) {
        if (restoredWindowWatermarks.containsKey(operator.getPlanNodeId()) && !operator.hasWindowAggregate()) {
            throw new IllegalArgumentException("Restored window clock requires a WindowAggregate stage");
        }
        if (operator.getPlanNodeId() <= 0 || !identities.add(operator.getPlanNodeId())) {
            throw new IllegalArgumentException("Native control tree needs unique positive stage identities");
        }
    }

    /** CommonExecUnion/UnionTransformation create no Flink operator between nested unions. */
    private void unionChannels(Operator operator, List<Operator> channels) {
        if (operator.hasUnion()) {
            register(operator);
            if (operator.getUnion().getInputsCount() == 0) {
                throw new IllegalArgumentException("A native UNION control edge has no input channels");
            }
            for (Operator child : operator.getUnion().getInputsList()) {
                unionChannels(child, channels);
            }
        } else {
            channels.add(operator);
        }
    }

    private final class Edge {
        final Node node;
        final int port;

        Edge(Node node, int port) {
            this.node = node;
            this.port = port;
        }
    }

    private final class Node {
        private final long id;
        private final List<Edge> parents = new ArrayList<>();
        private final IndexedCombinedWatermarkStatus combined;
        private final tech.streamfusion.flink.union.UnionInputWatermarks union;
        private long lastWatermark = Long.MIN_VALUE;
        private final boolean window;
        private final boolean[] ended;

        private Node(long id, Node parent, int parentPort, int arity, boolean unionInput, boolean window) {
            this.id = id;
            this.window = window;
            lastWatermark = restoredWindowWatermarks.getOrDefault(id, Long.MIN_VALUE);
            if (parent != null) parents.add(new Edge(parent, parentPort));
            ended = new boolean[arity];
            combined = unionInput ? null : IndexedCombinedWatermarkStatus.forInputsCount(arity);
            union = unionInput
                    ? new tech.streamfusion.flink.union.UnionInputWatermarks(
                            arity, watermark -> emitWatermark(watermark.getTimestamp()), this::emitStatus)
                    : null;
        }

        private void watermark(int port, long timestamp) throws Exception {
            // Flink input gauges observe arrivals, not the idle-aware emitted watermark.
            listener.inputWatermark(id, port, timestamp);
            if (union != null) {
                union.watermark(port, timestamp);
                return;
            }
            if (combined.updateWatermark(port, timestamp)) {
                emitWatermark(combined.getCombinedWatermark());
            }
        }

        private void endInput(int port) throws Exception {
            java.util.Objects.checkIndex(port, ended.length);
            if (ended[port]) return;
            ended[port] = true;
            for (boolean complete : ended) if (!complete) return;
            listener.endInput(id);
            for (var parent : parents) parent.node.endInput(parent.port);
        }

        private void status(int port, WatermarkStatus status) throws Exception {
            if (union != null) {
                union.status(port, status);
                return;
            }
            boolean wasIdle = combined.isIdle();
            // Match AbstractStreamOperatorV2: first advance the watermark, then emit idleness.
            if (combined.updateStatus(port, status.isIdle())) {
                emitWatermark(combined.getCombinedWatermark());
            }
            if (wasIdle != combined.isIdle()) {
                emitStatus(status);
            }
        }

        private void emitStatus(WatermarkStatus status) throws Exception {
            listener.status(id, status);
            for (var parent : parents) parent.node.status(parent.port, status);
        }

        private void emitWatermark(long timestamp) throws Exception {
            // WindowAggOperator forwards its restored clock when replay delivers an older
            // watermark. Input gauges still observe the actual arrival above.
            if (window) timestamp = Math.max(timestamp, lastWatermark);
            lastWatermark = timestamp;
            listener.watermark(id, timestamp);
            for (var parent : parents) parent.node.watermark(parent.port, timestamp);
        }
    }
}
