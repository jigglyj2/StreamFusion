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

    /** Validates the optional component before planner replacement and again on each worker. */
    public static String rocksDbUnsupportedReason() {
        return NativeRocksDbLibrary.unsupportedReason();
    }

    public static NativeStateBinding memory(long nodeId, int maxParallelism, int first, int last) {
        return binding(nodeId, maxParallelism, first, last)
                .setMemory(NativeMemoryState.getDefaultInstance())
                .build();
    }

    public static NativeStateBinding rocksDb(
            long nodeId, int maxParallelism, int first, int last, Path database, long flinkMemoryLease) {
        return rocksDb(nodeId, maxParallelism, first, last, database, flinkMemoryLease, null);
    }

    /** Automatic log relocation only; explicit user directories belong in NativeRocksDbOptions. */
    public static NativeStateBinding rocksDb(
            long nodeId,
            int maxParallelism,
            int first,
            int last,
            Path database,
            long flinkMemoryLease,
            Path logDirectory) {
        if (flinkMemoryLease <= 0) throw new IllegalArgumentException("A RocksDB Flink memory lease must be positive");
        var rocks = NativeRocksDbState.newBuilder()
                .setPluginPath(NativeRocksDbLibrary.path().toString())
                .setDatabasePath(database.toAbsolutePath().normalize().toString())
                .setMemoryLimit(flinkMemoryLease);
        if (logDirectory != null)
            rocks.setLogDirectory(logDirectory.toAbsolutePath().normalize().toString());
        return binding(nodeId, maxParallelism, first, last).setRocksdb(rocks).build();
    }

    public static byte[] serialize(List<NativeStateBinding> bindings) {
        return NativeStateBindings.newBuilder()
                .setProtocolVersion(protocolVersion(bindings))
                .addAllBindings(bindings)
                .build()
                .toByteArray();
    }

    /** Flink's existing IOManager assignment, shared by all state owners in a fused region. */
    public static byte[] serialize(List<NativeStateBinding> bindings, List<Path> spillDirectories) {
        if (spillDirectories.isEmpty()) throw new IllegalArgumentException("Flink spill directories must not be empty");
        return NativeStateBindings.newBuilder()
                .setProtocolVersion(Math.max(4, protocolVersion(bindings)))
                .addAllBindings(bindings)
                .addAllSpillDirectories(spillDirectories.stream()
                        .map(path -> path.toAbsolutePath().normalize().toString())
                        .collect(java.util.stream.Collectors.toList()))
                .build()
                .toByteArray();
    }

    private static int protocolVersion(List<NativeStateBinding> bindings) {
        int version = 1;
        for (var binding : bindings) {
            if (binding.hasRestoredWatermark()) version = Math.max(version, 3);
            if (!binding.hasRocksdb()) continue;
            var rocks = binding.getRocksdb();
            if (rocks.getStatisticsTickersCount() != 0) version = Math.max(version, 11);
            if (rocks.hasPartitionedIndexFilters()) version = Math.max(version, 8);
            if (rocks.hasLogDirectory()) version = Math.max(version, 2);
            if (rocks.hasWriteBufferRatio() || rocks.hasHighPriorityPoolRatio()) version = Math.max(version, 5);
            if (rocks.hasDatabaseOptions()) {
                var options = rocks.getDatabaseOptions();
                if (options.hasLogDirectory()) version = Math.max(version, 9);
                if (options.hasWriteBatchSize()) version = Math.max(version, 10);
                if (options.hasCompactionStyle()) version = Math.max(version, 8);
                version = Math.max(
                        version, options.hasLogLevel() || options.hasBloomFilter() || options.hasCompression() ? 7 : 6);
            }
        }
        return version;
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
