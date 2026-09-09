/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import tech.streamfusion.proto.plan.v1.NativeControlCapabilities;
import tech.streamfusion.proto.plan.v1.NativeControlEndInput;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;
import tech.streamfusion.proto.plan.v1.NativeStageControl;
import tech.streamfusion.proto.plan.v1.NativeStageControlCapability;

/** Coalesces each ordered Flink control wave into one native invocation, independent of operator types. */
final class NativeRegionControlScheduler {
    @FunctionalInterface
    interface Invocation {
        void run(byte[] request) throws Exception;
    }

    @FunctionalInterface
    private interface Action {
        void run() throws Exception;
    }

    private final NativeRegionControlTree tree;
    private final Invocation invocation;
    private final int inputCount;
    private final Map<Long, NativeStageControlCapability> capabilities = new LinkedHashMap<>();
    private final Map<Long, NativeStageControl> pending = new LinkedHashMap<>();
    private final List<Action> forwarding = new ArrayList<>();
    private boolean running;
    private boolean failed;

    NativeRegionControlScheduler(
            byte[] plan,
            int inputCount,
            byte[] encodedCapabilities,
            Invocation invocation,
            NativeRegionControlTree.Listener listener) {
        this(plan, inputCount, encodedCapabilities, Map.of(), invocation, listener);
    }

    NativeRegionControlScheduler(
            byte[] plan,
            int inputCount,
            byte[] encodedCapabilities,
            Map<Long, Long> restoredWindowWatermarks,
            Invocation invocation,
            NativeRegionControlTree.Listener listener) {
        this(
                wrapped -> new NativeRegionControlTree(plan, inputCount, restoredWindowWatermarks, wrapped),
                inputCount,
                encodedCapabilities,
                invocation,
                listener);
    }

    NativeRegionControlScheduler(
            tech.streamfusion.proto.plan.v1.NativeRegionPlan plan,
            byte[] encodedCapabilities,
            Map<Long, Long> restoredWindowWatermarks,
            Invocation invocation,
            NativeRegionControlTree.Listener listener) {
        this(
                wrapped -> new NativeRegionControlTree(plan, restoredWindowWatermarks, wrapped),
                plan.getInputCount(),
                encodedCapabilities,
                invocation,
                listener);
    }

    private NativeRegionControlScheduler(
            java.util.function.Function<NativeRegionControlTree.Listener, NativeRegionControlTree> factory,
            int inputCount,
            byte[] encodedCapabilities,
            Invocation invocation,
            NativeRegionControlTree.Listener listener) {
        this.inputCount = inputCount;
        this.invocation = java.util.Objects.requireNonNull(invocation);
        tree = factory.apply(new NativeRegionControlTree.Listener() {
            @Override
            public void inputWatermark(long id, int port, long timestamp) throws Exception {
                listener.inputWatermark(id, port, timestamp);
            }

            @Override
            public void watermark(long id, long timestamp) throws Exception {
                var capability = capabilities.get(id);
                if (capability != null && capability.getWatermark()) {
                    if (pending.containsKey(id)) flush();
                    pending.put(
                            id,
                            NativeStageControl.newBuilder()
                                    .setPlanNodeId(id)
                                    .setWatermarkMillis(timestamp)
                                    .build());
                }
                // Mini-batch output precedes the watermark and its output gauge update.
                forwarding.add(() -> listener.watermark(id, timestamp));
            }

            @Override
            public void status(long id, WatermarkStatus status) throws Exception {
                // All-idle input completion may first advance this subtree to its maximum
                // watermark, then advance an ancestor again when that subtree becomes idle.
                // Do not merge those two ordered watermarks into one native timer event.
                flush();
                forwarding.add(() -> listener.status(id, status));
            }

            @Override
            public void endInput(long id) {
                var capability = capabilities.get(id);
                if (capability != null && capability.getEndInput()) {
                    pending.put(
                            id,
                            NativeStageControl.newBuilder()
                                    .setPlanNodeId(id)
                                    .setEndInput(NativeControlEndInput.getDefaultInstance())
                                    .build());
                }
                forwarding.add(() -> listener.endInput(id));
            }

            @Override
            public void latency(long id, LatencyMarker marker) throws Exception {
                listener.latency(id, marker);
            }
        });
        try {
            var decoded = NativeControlCapabilities.parseFrom(encodedCapabilities);
            if (decoded.getProtocolVersion() != 1 && decoded.getProtocolVersion() != 2)
                throw new IllegalArgumentException("Unsupported native control capability version");
            for (var stage : decoded.getStagesList()) {
                if (decoded.getProtocolVersion() == 1 && stage.getProcessingTime())
                    throw new IllegalArgumentException("Processing-time capabilities require protocol 2");
                long id = stage.getPlanNodeId();
                if (id <= 0 || !tree.contains(id) || capabilities.putIfAbsent(id, stage) != null) {
                    throw new IllegalArgumentException("Native control capabilities require unique bound stage IDs");
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException error) {
            throw new IllegalArgumentException("Invalid native control capability protobuf", error);
        }
    }

    List<Long> outputIds() {
        return tree.outputIds();
    }

    long rootId() {
        return tree.rootId();
    }

    void requireHealthy() {
        if (failed) throw new IllegalStateException("Native control scheduling failed; requires recovery");
    }

    void watermark(int input, long timestamp) throws Exception {
        dispatch(() -> tree.watermark(input, timestamp));
    }

    /** A Flink timer callback targets its owning native stage; it never advances watermarks. */
    void processingTime(long id, long timestamp) throws Exception {
        dispatch(() -> {
            var capability = capabilities.get(id);
            if (capability == null || !capability.getProcessingTime())
                throw new IllegalArgumentException("Native stage has no processing-time timer capability: " + id);
            pending.put(
                    id,
                    NativeStageControl.newBuilder()
                            .setPlanNodeId(id)
                            .setProcessingTimeMillis(timestamp)
                            .build());
        });
    }

    void status(int input, WatermarkStatus status) throws Exception {
        dispatch(() -> tree.status(input, status));
    }

    void endInput(int input) throws Exception {
        dispatch(() -> tree.endInput(input));
    }

    void finish() throws Exception {
        dispatch(() -> {
            for (int input = 0; input < inputCount; input++) tree.endInput(input);
        });
    }

    void beforeCheckpoint(long checkpoint) throws Exception {
        dispatch(() -> {
            for (var stage : capabilities.values())
                if (stage.getBeforeCheckpoint()) {
                    pending.put(
                            stage.getPlanNodeId(),
                            NativeStageControl.newBuilder()
                                    .setPlanNodeId(stage.getPlanNodeId())
                                    .setBeforeCheckpoint(checkpoint)
                                    .build());
                }
        });
    }

    void latency(int input, LatencyMarker marker) throws Exception {
        requireHealthy();
        tree.latency(input, marker);
    }

    private void dispatch(Action arrival) throws Exception {
        requireHealthy();
        if (running) throw new IllegalStateException("Native region control dispatch is already active");
        running = true;
        try {
            arrival.run();
            flush();
        } catch (Exception | Error failure) {
            failed = true;
            throw failure;
        } finally {
            // Failed native output must never be followed by a forwarded barrier/watermark.
            pending.clear();
            forwarding.clear();
            running = false;
        }
    }

    private void flush() throws Exception {
        if (!pending.isEmpty())
            invocation.run(NativeControlInvocation.newBuilder()
                    .setProtocolVersion(
                            pending.values().stream().anyMatch(NativeStageControl::hasProcessingTimeMillis) ? 2 : 1)
                    .addAllStages(pending.values())
                    .build()
                    .toByteArray());
        for (Action action : forwarding) action.run();
        pending.clear();
        forwarding.clear();
    }
}
