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

    public NativeLocalWindowResources(Map<Long, FlinkOperatorMemoryShare> shares) {
        this.shares = Map.copyOf(shares);
        if (shares.keySet().stream().anyMatch(id -> id <= 0))
            throw new IllegalArgumentException("Local-window resources require stable positive plan-node identities");
    }

    public void validate(byte[] bytes) {
        try {
            var expected = new HashSet<Long>();
            collect(NativePlan.parseFrom(bytes).getRoot(), expected);
            if (!expected.equals(shares.keySet()))
                throw new IllegalArgumentException(
                        "Local-window memory shares must match the native plan exactly: expected " + expected
                                + ", supplied " + shares.keySet());
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native local-window resource plan", failure);
        }
    }

    public byte[] resolve(Environment environment, StreamConfig runtime) {
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
