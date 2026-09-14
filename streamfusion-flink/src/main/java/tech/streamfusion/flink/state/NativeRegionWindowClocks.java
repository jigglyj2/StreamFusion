/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import tech.streamfusion.flink.proto.NativePhysicalPlan;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;
import tech.streamfusion.proto.plan.v1.Operator;

/** Flink union-operator clocks, kept separate from each window's rescalable keyed state. */
public final class NativeRegionWindowClocks {
    private final Map<Long, ListState<Long>> states = new LinkedHashMap<>();
    private final Map<Long, Long> current = new LinkedHashMap<>();
    private final Map<Long, Long> restored = new LinkedHashMap<>();

    NativeRegionWindowClocks(StateInitializationContext context, byte[] plan, List<Long> stateIds) throws Exception {
        var root = NativePlan.parseFrom(plan);
        if (root.hasRoot()) initialize(context, root.getRoot(), stateIds);
    }

    NativeRegionWindowClocks(
            StateInitializationContext context,
            tech.streamfusion.proto.plan.v1.NativeRegionPlan plan,
            List<Long> stateIds)
            throws Exception {
        tech.streamfusion.flink.operator.NativeRegionPlanComposer.validate(plan);
        for (var stage : plan.getStagesList()) initialize(context, stage.getOperator(), stateIds);
    }

    public static java.util.Set<String> stateNames(byte[] bytes, List<Long> stateIds, boolean shared) {
        try {
            var names = new java.util.HashSet<String>();
            if (shared) {
                var plan = tech.streamfusion.proto.plan.v1.NativeRegionPlan.parseFrom(bytes);
                for (var stage : plan.getStagesList()) collectNames(stage.getOperator(), stateIds, names);
            } else {
                var plan = NativePlan.parseFrom(bytes);
                if (plan.hasRoot()) collectNames(plan.getRoot(), stateIds, names);
            }
            return java.util.Set.copyOf(names);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native window-clock plan", failure);
        }
    }

    private static void collectNames(Operator node, List<Long> ids, java.util.Set<String> names) {
        if (node.hasWindowAggregate() && ids.contains(node.getPlanNodeId())) names.add(stateName(node.getPlanNodeId()));
        for (var child : NativePhysicalPlan.children(node)) collectNames(child, ids, names);
    }

    private static String stateName(long id) {
        return "streamfusion-window-watermark-v1-" + id;
    }

    private void initialize(StateInitializationContext context, Operator node, List<Long> stateIds) throws Exception {
        if (node.hasWindowAggregate() && stateIds.contains(node.getPlanNodeId())) {
            long id = node.getPlanNodeId();
            if (states.containsKey(id)) throw new IllegalArgumentException("Duplicate window clock identity " + id);
            // A fused Flink operator contains several original operators. Namespace their
            // union state by the same stable identity used for native keyed state and metrics.
            var state = context.getOperatorStateStore()
                    .getUnionListState(new ListStateDescriptor<>(stateName(id), LongSerializer.INSTANCE));
            long watermark = Long.MIN_VALUE;
            if (context.isRestored()) {
                boolean found = false;
                for (Long value : state.get()) {
                    watermark = found ? Math.min(watermark, value) : value;
                    found = true;
                }
                restored.put(id, watermark);
            }
            states.put(id, state);
            current.put(id, watermark);
        }
        for (var child : NativePhysicalPlan.children(node)) initialize(context, child, stateIds);
    }

    NativeStateBinding bind(NativeStateBinding binding) {
        Long watermark = restored.get(binding.getPlanNodeId());
        return watermark == null
                ? binding
                : binding.toBuilder().setRestoredWatermark(watermark).build();
    }

    Map<Long, Long> restored() {
        return Map.copyOf(restored);
    }

    void watermark(long id, long value) {
        current.computeIfPresent(id, (ignored, previous) -> Math.max(previous, value));
    }

    void snapshot() throws Exception {
        for (var entry : states.entrySet()) entry.getValue().update(List.of(current.get(entry.getKey())));
    }
}
