/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.window;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.flink.proto.NativePhysicalPlan;
import tech.streamfusion.proto.plan.v1.NativeLocalWindowBuffer;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeTaskBinding;
import tech.streamfusion.proto.plan.v1.NativeTaskBindings;
import tech.streamfusion.proto.plan.v1.Operator;

/** Resolves every local-window capacity from original Flink resource metadata at task initialization. */
public final class NativeLocalWindowResources implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final NativeLocalWindowResources NONE = new NativeLocalWindowResources(Map.of());
    private final Map<Long, FlinkOperatorMemoryShare> shares;
    private final Set<Long> owners;

    public NativeLocalWindowResources(Map<Long, FlinkOperatorMemoryShare> shares) {
        this.shares = Map.copyOf(shares);
        this.owners = Set.copyOf(this.shares.keySet());
        if (owners.stream().anyMatch(id -> id <= 0))
            throw new IllegalArgumentException("Local-window resources require stable positive plan-node identities");
    }

    private NativeLocalWindowResources(Set<Long> owners) {
        this.owners = Set.copyOf(owners);
        this.shares = null;
    }

    public static NativeLocalWindowResources pending(byte[] plan) {
        var owners = owners(plan);
        return owners.isEmpty() ? NONE : new NativeLocalWindowResources(owners);
    }

    public boolean isPending() {
        return shares == null;
    }

    public NativeLocalWindowResources resolvedFrom(Map<Long, FlinkOperatorMemoryShare> pipelineShares) {
        if (!isPending()) return this;
        var selected = new java.util.HashMap<Long, FlinkOperatorMemoryShare>();
        for (long owner : owners) {
            var share = pipelineShares.get(owner);
            if (share == null)
                throw new IllegalArgumentException("Complete pipeline is missing local-window share " + owner);
            selected.put(owner, share);
        }
        return new NativeLocalWindowResources(selected);
    }

    public void validate(byte[] bytes) {
        var expected = owners(bytes);
        if (!expected.equals(owners))
            throw new IllegalArgumentException(
                    "Local-window memory shares must match the native plan exactly: expected " + expected
                            + ", supplied " + owners);
    }

    private static Set<Long> owners(byte[] bytes) {
        try {
            var expected = new HashSet<Long>();
            collect(NativePlan.parseFrom(bytes).getRoot(), expected);
            return expected;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native local-window resource plan", failure);
        }
    }

    private void writeObject(java.io.ObjectOutputStream output) throws java.io.IOException {
        if (isPending())
            throw new java.io.NotSerializableException("Local-window shares require complete-pipeline finalization");
        output.defaultWriteObject();
    }

    public byte[] resolve(Environment environment, StreamConfig runtime) {
        if (isPending()) throw new IllegalStateException("Local-window shares require complete-pipeline finalization");
        if (shares.isEmpty()) return null;
        var result = NativeTaskBindings.newBuilder().setProtocolVersion(1);
        shares.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            long bytes = entry.getValue().memoryBytes(environment, runtime);
            if (bytes <= 0) throw new IllegalStateException("Flink assigned no original local-window buffer memory");
            result.addBindings(NativeTaskBinding.newBuilder()
                    .setPlanNodeId(entry.getKey())
                    .setLocalWindowBuffer(NativeLocalWindowBuffer.newBuilder()
                            .setFlinkBufferMemoryBytes(bytes)
                            .setFlinkPageBytes(environment.getMemoryManager().getPageSize())));
        });
        return result.build().toByteArray();
    }

    private static void collect(Operator node, Set<Long> locals) {
        if (node.hasLocalWindowAggregate() && (node.getPlanNodeId() <= 0 || !locals.add(node.getPlanNodeId())))
            throw new IllegalArgumentException("Local-window resource owners must have unique positive identities");
        for (var child : NativePhysicalPlan.children(node)) collect(child, locals);
    }
}
