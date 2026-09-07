/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.nio.file.Path;
import java.util.List;
import tech.streamfusion.proto.plan.v1.NativeMemoryState;
import tech.streamfusion.proto.plan.v1.NativeRocksDbState;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;
import tech.streamfusion.proto.plan.v1.NativeStateBindings;

/** Encodes Flink's task-local state resource assignments, not a separate memory configuration. */
public final class NativeStateResources {
    private NativeStateResources() {}

    public static NativeStateBinding memory(long nodeId, int maxParallelism, int first, int last) {
        return binding(nodeId, maxParallelism, first, last)
                .setMemory(NativeMemoryState.getDefaultInstance())
                .build();
    }

    public static NativeStateBinding rocksDb(
            long nodeId, int maxParallelism, int first, int last, Path database, long flinkMemoryLease) {
        if (flinkMemoryLease <= 0) throw new IllegalArgumentException("A RocksDB Flink memory lease must be positive");
        return binding(nodeId, maxParallelism, first, last)
                .setRocksdb(NativeRocksDbState.newBuilder()
                        .setPluginPath(NativeRocksDbLibrary.path().toString())
                        .setDatabasePath(database.toAbsolutePath().normalize().toString())
                        .setMemoryLimit(flinkMemoryLease))
                .build();
    }

    public static byte[] serialize(List<NativeStateBinding> bindings) {
        return NativeStateBindings.newBuilder()
                .setProtocolVersion(1)
                .addAllBindings(bindings)
                .build()
                .toByteArray();
    }

    private static NativeStateBinding.Builder binding(long nodeId, int maxParallelism, int first, int last) {
        if (nodeId <= 0
                || maxParallelism <= 0
                || maxParallelism > 32768
                || first < 0
                || first > last
                || last >= maxParallelism) {
            throw new IllegalArgumentException("Invalid native state plan-node ID or Flink key-group range");
        }
        return NativeStateBinding.newBuilder()
                .setPlanNodeId(nodeId)
                .setMaxParallelism(maxParallelism)
                .setFirstKeyGroup(first)
                .setLastKeyGroup(last);
    }
}
