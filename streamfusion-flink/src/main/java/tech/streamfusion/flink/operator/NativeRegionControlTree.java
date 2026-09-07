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

/** Flink control-event propagation for a native tree of control-preserving physical stages. */
final class NativeRegionControlTree {
    interface Listener {
        default void inputWatermark(long nodeId, int port, long timestamp) throws Exception {}

        default void endInput(long nodeId) throws Exception {}

        void watermark(long nodeId, long timestamp) throws Exception;

        void status(long nodeId, WatermarkStatus status) throws Exception;

        void latency(long nodeId, LatencyMarker marker) throws Exception;
    }

    private final Node[] inputs;
    private final Listener listener;
    private final Set<Long> identities = new HashSet<>();
    private final Node root;

    NativeRegionControlTree(byte[] identifiedPlan, int inputCount, Listener listener) {
        if (inputCount <= 0) {
            throw new IllegalArgumentException("A native control tree needs external inputs");
        }
        inputs = new Node[inputCount];
        this.listener = java.util.Objects.requireNonNull(listener);
        try {
            NativePlan plan = NativePlan.parseFrom(identifiedPlan);
            if (!plan.hasRoot()) {
                throw new IllegalArgumentException("Native control tree is missing its root");
            }
            root = build(plan.getRoot(), null, 0);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native control plan", failure);
        }
        for (Node input : inputs) {
            if (input == null) {
                throw new IllegalArgumentException("Native control tree has an unbound external input");
            }
        }
    }

    long rootId() {
        return root.id;
    }

    long watermark() {
        return root.lastWatermark;
    }

    void watermark(int input, long timestamp) throws Exception {
        port(input).watermark(0, timestamp);
    }

    void status(int input, WatermarkStatus status) throws Exception {
        port(input).status(0, status);
    }

    void endInput(int input) throws Exception {
        port(input).endInput(0);
    }

    boolean contains(long id) {
        return identities.contains(id);
    }

    void latency(int input, LatencyMarker marker) throws Exception {
        for (Node node = port(input); node != null; node = node.parent) {
            listener.latency(node.id, marker);
        }
    }

    private Node port(int input) {
        return inputs[java.util.Objects.checkIndex(input, inputs.length)];
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
        Node node = new Node(id, parent, parentPort, input ? 1 : children.size(), operator.hasUnion());
        if (input) {
            int port = operator.getInput().getInputIndex();
            if (port < 0 || port >= inputs.length || inputs[port] != null) {
                throw new IllegalArgumentException("Native control input slots must be unique and in range");
            }
            inputs[port] = node;
        }
        for (int index = 0; index < children.size(); index++) {
            build(children.get(index), node, index);
        }
        return node;
    }

    private void register(Operator operator) {
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

    private final class Node {
        private final long id;
        private final Node parent;
        private final int parentPort;
        private final IndexedCombinedWatermarkStatus combined;
        private final tech.streamfusion.flink.union.UnionInputWatermarks union;
        private long lastWatermark = Long.MIN_VALUE;
        private final boolean[] ended;

        private Node(long id, Node parent, int parentPort, int arity, boolean unionInput) {
            this.id = id;
            this.parent = parent;
            this.parentPort = parentPort;
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
            if (parent != null) parent.endInput(parentPort);
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
            if (parent != null) {
                parent.status(parentPort, status);
            }
        }

        private void emitWatermark(long timestamp) throws Exception {
            lastWatermark = timestamp;
            listener.watermark(id, timestamp);
            if (parent != null) {
                parent.watermark(parentPort, timestamp);
            }
        }
    }
}
