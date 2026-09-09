/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.join;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import tech.streamfusion.flink.arrow.ArrowLookupSnapshotBindings;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;
import tech.streamfusion.flink.proto.NativePhysicalPlan;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Serializable source descriptions; no file is opened until the runtime operator opens. */
public final class NativeLookupSources implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final NativeLookupSources NONE = new NativeLookupSources(Map.of());
    private final Map<Long, CsvLookupSnapshotSource> sources;

    public NativeLookupSources(Map<Long, CsvLookupSnapshotSource> sources) {
        this.sources = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(sources));
        if (sources.entrySet().stream()
                .anyMatch(entry -> entry.getKey() == null || entry.getKey() <= 0 || entry.getValue() == null))
            throw new IllegalArgumentException(
                    "Lookup sources require positive physical identities and source descriptions");
    }

    public boolean isEmpty() {
        return sources.isEmpty();
    }

    public void validate(byte[] bytes, boolean region) {
        try {
            var expected = new HashSet<Long>();
            if (region) {
                for (var stage : NativeRegionPlan.parseFrom(bytes).getStagesList())
                    collect(stage.getOperator(), expected);
            } else collect(NativePlan.parseFrom(bytes).getRoot(), expected);
            if (!expected.equals(sources.keySet()))
                throw new IllegalArgumentException("Lookup sources must match the physical plan exactly");
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid lookup source physical plan", failure);
        }
    }

    private static void collect(Operator node, java.util.Set<Long> ids) {
        if (node.hasLookupJoin() && (node.getPlanNodeId() <= 0 || !ids.add(node.getPlanNodeId())))
            throw new IllegalArgumentException("Lookup physical source identities must be unique and positive");
        for (var child : NativePhysicalPlan.children(node)) collect(child, ids);
    }

    public NativeExecutionContext open(
            byte[] plan,
            NativeMemoryManager memory,
            byte[] task,
            boolean region,
            BufferAllocator allocator,
            ClassLoader loader)
            throws Exception {
        validate(plan, region);
        // Internal source chunk geometry, not a deployment option. Probe batching remains Flink-owned.
        return ArrowLookupSnapshotBindings.create(plan, memory, null, task, region, sources, allocator, loader, 1024);
    }
}
