/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeStateResources;

/** Flink resource and checkpoint lifecycle for a shared native tree, independent of operator family. */
public final class NativeRegionStateLifecycle implements AutoCloseable {
    private StreamFusionTaskMemory memory;
    private NativeMemoryManager manager;
    private NativeRegionStateParticipant participant;
    private StreamFusionKeyedStateBackend<?> backend;
    private Path directory;
    private long fallbackReservation;
    private boolean rawSnapshot = true;
    private NativeRegionWindowClocks windowClocks;

    public void initialize(
            StateInitializationContext initialization,
            Environment environment,
            StreamConfig config,
            OperatorMetricGroup metrics,
            KeyedStateBackend<?> keyedBackend,
            int maxParallelism,
            byte[] plan,
            List<Long> stateIds)
            throws Exception {
        if (manager != null || stateIds.isEmpty()) {
            throw new IllegalStateException("Native region state must initialize once with state-node identities");
        }
        if (!(keyedBackend instanceof CheckpointableKeyedStateBackend)) {
            throw new IllegalStateException("A stateful native region requires a checkpointable keyed backend");
        }
        var range = ((CheckpointableKeyedStateBackend<?>) keyedBackend).getKeyGroupRange();
        String type = keyedBackend.getBackendTypeIdentifier();
        boolean rocks = "rocksdb".equals(type)
                || ("batch".equals(type)
                        && "rocksdb".equals(environment.getJobConfiguration().get(StateBackendOptions.STATE_BACKEND)));
        if (!rocks && !"hashmap".equals(type) && !"batch".equals(type)) {
            throw new IllegalStateException("Unsupported native region backend " + type);
        }
        backend = keyedBackend instanceof StreamFusionKeyedStateBackend
                ? (StreamFusionKeyedStateBackend<?>) keyedBackend
                : null;
        try {
            windowClocks = new NativeRegionWindowClocks(initialization, plan, stateIds);
            directory = Files.createTempDirectory(
                    environment.getIOManager().getSpillingDirectories()[0].toPath(), "streamfusion-region-state-");
            memory = StreamFusionTaskMemory.createWithState(
                    environment, config, metrics, "streamfusion-native-region", plan, assigned -> {
                        manager = assigned;
                        if (backend != null && backend.nativeRocksDbMemoryScope() != null) {
                            ((tech.streamfusion.flink.memory.FlinkManagedMemory) assigned)
                                    .shareRocksDbMemoryScope(backend.nativeRocksDbMemoryScope());
                        }
                        long stateLease = backend == null ? 0 : backend.nativeRocksDbMemoryLimit();
                        if (rocks && stateLease == 0) {
                            stateLease = assigned.limit() / 4;
                            if (!assigned.tryReserve(stateLease)) {
                                throw new IllegalStateException("Flink denied the native region RocksDB lease");
                            }
                            fallbackReservation = stateLease;
                        }
                        // The plugin shares its cache/write-buffer manager for this lease, as it
                        // does for separate Flink native state owners. Do not multiply the budget.
                        final long lease = stateLease;
                        return NativeStateResources.serialize(stateIds.stream()
                                .map(id -> rocks
                                        ? NativeStateResources.rocksDb(
                                                id,
                                                maxParallelism,
                                                range.getStartKeyGroup(),
                                                range.getEndKeyGroup(),
                                                directory.resolve("node-" + id),
                                                lease,
                                                NativeRocksDbLogDirectory.resolve(directory.resolve("node-" + id)))
                                        : NativeStateResources.memory(
                                                id, maxParallelism, range.getStartKeyGroup(), range.getEndKeyGroup()))
                                .map(windowClocks::bind)
                                .collect(Collectors.toList()));
                    });
            // Flink owns staged checkpoint cleanup after asynchronous upload. Keep those files
            // outside the live database directory that this lifecycle deletes on close.
            participant = new NativeRegionStateParticipant(
                    memory.executionContext().state(), stateIds, range, directory.getParent(), manager);
            if (backend != null) backend.registerNativeStateParticipant(participant, rocks);
            participant.restoreRawState(initialization);
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    public StreamFusionTaskMemory memory() {
        return memory;
    }

    public java.util.Map<Long, Long> restoredWindowWatermarks() {
        return windowClocks.restored();
    }

    public void watermark(long nodeId, long timestamp) {
        windowClocks.watermark(nodeId, timestamp);
    }

    public void beginSnapshot(CheckpointOptions options) {
        rawSnapshot = backend == null
                || !backend.usesNativeIncrementalCheckpoints()
                || options.getCheckpointType().isSavepoint();
    }

    public void writeSnapshot(StateSnapshotContext context) throws Exception {
        windowClocks.snapshot();
        if (rawSnapshot) participant.writeRawSnapshot(context);
    }

    public void finishSnapshot() {
        rawSnapshot = true;
    }

    @Override
    public void close() throws Exception {
        try {
            // Close native databases before returning a fallback cache lease to Flink.
            if (memory != null) memory.executionContext().close();
        } finally {
            try {
                if (fallbackReservation != 0) {
                    manager.release(fallbackReservation);
                    fallbackReservation = 0;
                }
            } finally {
                try {
                    if (memory != null) memory.close();
                } finally {
                    memory = null;
                    if (directory != null) {
                        try (var paths = Files.walk(directory)) {
                            for (var path :
                                    paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
                                Files.deleteIfExists(path);
                        }
                        directory = null;
                    }
                }
            }
        }
    }
}
