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
    private final java.util.function.Function<tech.streamfusion.nativebridge.NativeExecutionContext, AutoCloseable>
            beforeRestore;
    private AutoCloseable metricsRegistration;
    private StreamFusionTaskMemory memory;
    private NativeMemoryManager manager;
    private NativeRegionStateParticipant participant;
    private StreamFusionKeyedStateBackend<?> backend;
    private Path directory;
    private long fallbackReservation;
    private boolean rawSnapshot = true;
    private NativeRegionWindowClocks windowClocks;

    public NativeRegionStateLifecycle() {
        this(ignored -> null);
    }

    /** Register task-owned native metrics after DB creation and before any restore work. */
    public NativeRegionStateLifecycle(
            java.util.function.Function<tech.streamfusion.nativebridge.NativeExecutionContext, AutoCloseable>
                    beforeRestore) {
        this.beforeRestore = java.util.Objects.requireNonNull(beforeRestore);
    }

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
        initialize(initialization, environment, config, metrics, keyedBackend, maxParallelism, plan, stateIds, null);
    }

    public void initialize(
            StateInitializationContext initialization,
            Environment environment,
            StreamConfig config,
            OperatorMetricGroup metrics,
            KeyedStateBackend<?> keyedBackend,
            int maxParallelism,
            byte[] plan,
            List<Long> stateIds,
            byte[] taskBindings)
            throws Exception {
        initialize(
                initialization,
                environment,
                config,
                metrics,
                keyedBackend,
                maxParallelism,
                plan,
                stateIds,
                taskBindings,
                false);
    }

    public void initializeRegion(
            StateInitializationContext initialization,
            Environment environment,
            StreamConfig config,
            OperatorMetricGroup metrics,
            KeyedStateBackend<?> keyedBackend,
            int maxParallelism,
            byte[] plan,
            List<Long> stateIds,
            byte[] taskBindings)
            throws Exception {
        initialize(
                initialization,
                environment,
                config,
                metrics,
                keyedBackend,
                maxParallelism,
                plan,
                stateIds,
                taskBindings,
                true);
    }

    private void initialize(
            StateInitializationContext initialization,
            Environment environment,
            StreamConfig config,
            OperatorMetricGroup metrics,
            KeyedStateBackend<?> keyedBackend,
            int maxParallelism,
            byte[] plan,
            List<Long> stateIds,
            byte[] taskBindings,
            boolean sharedRegion)
            throws Exception {
        initialize(
                initialization,
                environment,
                config,
                metrics,
                keyedBackend,
                maxParallelism,
                plan,
                stateIds,
                taskBindings,
                sharedRegion,
                true);
    }

    public void prepare(
            StateInitializationContext initialization,
            Environment environment,
            StreamConfig config,
            OperatorMetricGroup metrics,
            KeyedStateBackend<?> keyedBackend,
            int maxParallelism,
            byte[] plan,
            List<Long> stateIds,
            byte[] taskBindings,
            boolean sharedRegion)
            throws Exception {
        initialize(
                initialization,
                environment,
                config,
                metrics,
                keyedBackend,
                maxParallelism,
                plan,
                stateIds,
                taskBindings,
                sharedRegion,
                false);
    }

    private void initialize(
            StateInitializationContext initialization,
            Environment environment,
            StreamConfig config,
            OperatorMetricGroup metrics,
            KeyedStateBackend<?> keyedBackend,
            int maxParallelism,
            byte[] plan,
            List<Long> stateIds,
            byte[] taskBindings,
            boolean sharedRegion,
            boolean readRawState)
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
            windowClocks = sharedRegion
                    ? new NativeRegionWindowClocks(
                            initialization, tech.streamfusion.proto.plan.v1.NativeRegionPlan.parseFrom(plan), stateIds)
                    : new NativeRegionWindowClocks(initialization, plan, stateIds);
            Path stateRoot = rocks
                    ? backend != null && backend.nativeRocksDbStorageRoot() != null
                            ? backend.nativeRocksDbStorageRoot()
                            : environment
                                    .getTaskManagerInfo()
                                    .getTmpWorkingDirectory()
                                    .toPath()
                    : environment.getIOManager().getSpillingDirectories()[0].toPath();
            // Resolve symlinks and parent components before the native binding normalizes
            // paths. Lexical normalization alone can select a different filesystem root.
            directory = Files.createTempDirectory(
                    Files.createDirectories(stateRoot).toRealPath(), "streamfusion-region-state-");
            java.util.function.Function<NativeMemoryManager, byte[]> bindings = assigned -> {
                manager = assigned;
                if (backend != null && backend.nativeRocksDbMemoryScope() != null) {
                    ((tech.streamfusion.flink.memory.FlinkManagedMemory) assigned)
                            .shareRocksDbMemoryScope(backend.nativeRocksDbMemoryScope());
                }
                long stateLease = backend == null ? 0 : backend.nativeRocksDbMemoryLimit();
                if (rocks && stateLease == 0) {
                    stateLease =
                            ((tech.streamfusion.flink.memory.FlinkManagedMemory) assigned).assignedOperatorShare() / 4;
                    if (!assigned.tryReserve(stateLease)) {
                        throw new IllegalStateException("Flink denied the native region RocksDB lease");
                    }
                    fallbackReservation = stateLease;
                }
                // The plugin shares its cache/write-buffer manager for this lease, as it
                // does for separate Flink native state owners. Do not multiply the budget.
                final long lease = stateLease;
                return NativeStateResources.serialize(
                        stateIds.stream()
                                .map(id -> rocks
                                        ? NativeStateResources.rocksDb(
                                                id,
                                                maxParallelism,
                                                range.getStartKeyGroup(),
                                                range.getEndKeyGroup(),
                                                directory.resolve("node-" + id),
                                                lease,
                                                backend == null
                                                        ? NativeRocksDbLogDirectory.resolve(
                                                                directory.resolve("node-" + id))
                                                        : backend.nativeRocksDbDefaultLogDirectory(
                                                                directory.resolve("node-" + id)))
                                        : NativeStateResources.memory(
                                                id, maxParallelism, range.getStartKeyGroup(), range.getEndKeyGroup()))
                                .map(binding ->
                                        backend == null ? binding : backend.bindNativeRocksDbConfiguration(binding))
                                .map(windowClocks::bind)
                                .collect(Collectors.toList()),
                        java.util.Arrays.stream(environment.getIOManager().getSpillingDirectories())
                                .map(java.io.File::toPath)
                                .collect(Collectors.toList()));
            };
            memory = sharedRegion
                    ? StreamFusionTaskMemory.createRegionWithState(
                            environment, config, metrics, "streamfusion-native-region", plan, bindings, taskBindings)
                    : StreamFusionTaskMemory.createWithState(
                            environment, config, metrics, "streamfusion-native-region", plan, bindings, taskBindings);
            metricsRegistration = beforeRestore.apply(memory.executionContext());
            // Flink owns staged checkpoint cleanup after asynchronous upload. Keep those files
            // outside the live database directory that this lifecycle deletes on close.
            participant = new NativeRegionStateParticipant(
                    memory.executionContext().state(), stateIds, range, directory.getParent());
            if (backend != null) backend.registerNativeStateParticipant(participant, rocks);
            if (readRawState) participant.restoreRawState(initialization);
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Attach Flink's actual operator state after an earlier physical restore in backend creation. */
    public void finishPreparedInitialization(
            StateInitializationContext initialization,
            KeyedStateBackend<?> keyedBackend,
            byte[] plan,
            List<Long> stateIds,
            boolean sharedRegion)
            throws Exception {
        if (memory == null || backend != keyedBackend)
            throw new IllegalStateException("Native prepared backend changed");
        var actual = sharedRegion
                ? new NativeRegionWindowClocks(
                        initialization, tech.streamfusion.proto.plan.v1.NativeRegionPlan.parseFrom(plan), stateIds)
                : new NativeRegionWindowClocks(initialization, plan, stateIds);
        if (!actual.restored().equals(windowClocks.restored())) {
            throw new IllegalStateException("Flink restored different union clocks after native state preparation");
        }
        windowClocks = actual;
        participant.restoreRawState(initialization);
        // The operator now owns the context and its Arrow allocator. Closing a keyed backend
        // must not invalidate batches still retained by the operator's dispatcher.
        backend.claimPreparedNativeRegion();
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
                || !backend.usesNativeFileCheckpoints()
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
            // Stop sampling before closing DBs, including initialization/restore failure.
            // The region may already have closed this registration; it is idempotent.
            org.apache.flink.util.IOUtils.closeAll(
                    metricsRegistration, memory == null ? null : memory.executionContext());
            metricsRegistration = null;
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
