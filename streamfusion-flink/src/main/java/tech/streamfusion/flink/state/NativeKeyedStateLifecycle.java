/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.StreamFusionStatefulOperatorMetrics;
import tech.streamfusion.nativebridge.NativeKeyedStateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Shared implementation of native keyed state lifecycle independent of the Flink operator API generation. */
final class NativeKeyedStateLifecycle implements Serializable {
    private final byte[] serializedPlan;
    private final String stateName;
    private final NativeKeyedStateBridge bridge;

    private transient long nativeHandle;
    private transient KeyGroupRange keyGroupRange;
    private transient FlinkManagedMemory managedMemory;
    private transient BufferAllocator allocator;
    private transient Path rocksDbDirectory;
    private transient long rocksDbManagedMemory;
    private transient long rocksDbOperatorReservation;
    private transient boolean writeRawKeyedSnapshot = true;
    private transient long rawSnapshotBytes;
    private transient StreamFusionStatefulOperatorMetrics statefulMetrics;
    private transient Map<Long, CheckpointObservation> fileCheckpoints;

    NativeKeyedStateLifecycle(byte[] serializedPlan, String stateName, NativeKeyedStateBridge bridge) {
        this.serializedPlan = serializedPlan.clone();
        this.stateName = stateName;
        this.bridge = bridge;
    }

    void initialize(
            StateInitializationContext context,
            Environment environment,
            StreamConfig operatorConfig,
            OperatorMetricGroup metricGroup,
            KeyedStateBackend<?> keyedStateBackend,
            int maxParallelism,
            NativeIncrementalStateParticipant participant)
            throws Exception {
        if (!(keyedStateBackend instanceof CheckpointableKeyedStateBackend)) {
            throw new IllegalStateException("Native " + stateName + " requires a checkpointable keyed state backend");
        }
        keyGroupRange = ((CheckpointableKeyedStateBackend<?>) keyedStateBackend).getKeyGroupRange();
        managedMemory = FlinkManagedMemory.create(
                environment, operatorConfig, metricGroup, "streamfusion-" + stateName.replace(' ', '-'));
        if (keyedStateBackend instanceof StreamFusionKeyedStateBackend) {
            var scope = ((StreamFusionKeyedStateBackend<?>) keyedStateBackend).nativeRocksDbMemoryScope();
            if (scope != null) managedMemory.shareRocksDbMemoryScope(scope);
        }
        allocator = managedMemory.allocator();
        String backendType = keyedStateBackend.getBackendTypeIdentifier();
        boolean useRocksDb = "rocksdb".equals(backendType)
                || ("batch".equals(backendType)
                        && "rocksdb".equals(environment.getJobConfiguration().get(StateBackendOptions.STATE_BACKEND)));
        if (useRocksDb) {
            Path spillDirectory =
                    environment.getIOManager().getSpillingDirectories()[0].toPath();
            rocksDbDirectory = Files.createTempDirectory(spillDirectory, "streamfusion-rocksdb-");
            long stateBackendMemory = keyedStateBackend instanceof StreamFusionKeyedStateBackend
                    ? ((StreamFusionKeyedStateBackend<?>) keyedStateBackend).nativeRocksDbMemoryLimit()
                    : 0;
            // Embedded runners may not provide the separately weighted STATE_BACKEND lease.
            long rocksDbMemory =
                    stateBackendMemory > 0 ? stateBackendMemory : managedMemory.assignedOperatorShare() / 4;
            boolean operatorMemoryFallback = stateBackendMemory == 0;
            if (operatorMemoryFallback && !managedMemory.tryReserve(rocksDbMemory)) {
                throw new IllegalStateException("Flink denied " + rocksDbMemory + " bytes for native RocksDB state");
            }
            try {
                nativeHandle = bridge.createRocksDb(
                        serializedPlan,
                        maxParallelism,
                        keyGroupRange.getStartKeyGroup(),
                        keyGroupRange.getEndKeyGroup(),
                        rocksDbDirectory,
                        rocksDbMemory,
                        managedMemory);
                rocksDbManagedMemory = rocksDbMemory;
                rocksDbOperatorReservation = operatorMemoryFallback ? rocksDbMemory : 0;
            } catch (RuntimeException failure) {
                if (operatorMemoryFallback) {
                    managedMemory.release(rocksDbMemory);
                }
                throw failure;
            }
        } else if ("hashmap".equals(backendType) || "batch".equals(backendType)) {
            // Flink's bounded runtime exposes its in-memory keyed backend as "batch". Native
            // bounded operators still own opaque canonical key-group state, so it has the same
            // direct-memory implementation and restore format as the hashmap backend.
            nativeHandle = bridge.createMemory(
                    serializedPlan,
                    maxParallelism,
                    keyGroupRange.getStartKeyGroup(),
                    keyGroupRange.getEndKeyGroup(),
                    managedMemory);
        } else {
            throw new IllegalStateException("Native "
                    + stateName
                    + " supports Flink hashmap, batch-memory, and RocksDB state backends, got "
                    + backendType);
        }
        statefulMetrics = new StreamFusionStatefulOperatorMetrics(metricGroup, useRocksDb);
        metricGroup.addGroup("StreamFusion").gauge("rocksDbSharedManagedMemoryReserved", () -> rocksDbManagedMemory);
        fileCheckpoints = new ConcurrentHashMap<>();
        if (keyedStateBackend instanceof StreamFusionKeyedStateBackend) {
            ((StreamFusionKeyedStateBackend<?>) keyedStateBackend)
                    .registerNativeStateParticipant(participant, useRocksDb);
        }
        restoreRawState(context);
    }

    private void restoreRawState(StateInitializationContext context) throws Exception {
        long restoreStarted = System.nanoTime();
        long restoredBytes = 0;
        boolean restored = false;
        try {
            for (KeyGroupStatePartitionStreamProvider provider : context.getRawKeyedStateInputs()) {
                restored = true;
                DataInputStream input = new DataInputStream(provider.getStream());
                int length = input.readInt();
                if (length < 0) {
                    throw new IOException(
                            "Negative native " + stateName + " state length for key group " + provider.getKeyGroupId());
                }
                byte[] state = new byte[length];
                input.readFully(state);
                restoredBytes += Integer.BYTES + length;
                bridge.restore(nativeHandle, provider.getKeyGroupId(), state);
            }
            if (restored) {
                statefulMetrics.restored(restoredBytes, System.nanoTime() - restoreStarted);
            }
        } catch (Throwable failure) {
            statefulMetrics.restoreFailed();
            throw failure;
        }
    }

    long nativeHandle() {
        return nativeHandle;
    }

    BufferAllocator allocator() {
        return allocator;
    }

    NativeMemoryManager memoryManager() {
        return managedMemory;
    }

    long managedMemoryUsed() {
        return managedMemory.reserved();
    }

    StreamFusionStatefulOperatorMetrics metrics() {
        return statefulMetrics;
    }

    long beginSnapshot(long checkpointId, CheckpointOptions options, KeyedStateBackend<?> keyedStateBackend) {
        boolean nativeFiles = keyedStateBackend instanceof StreamFusionKeyedStateBackend
                && ((StreamFusionKeyedStateBackend<?>) keyedStateBackend).usesNativeFileCheckpoints()
                && !options.getCheckpointType().isSavepoint();
        writeRawKeyedSnapshot = !nativeFiles;
        rawSnapshotBytes = 0;
        long startedNanos = System.nanoTime();
        if (nativeFiles) {
            fileCheckpoints.put(
                    checkpointId,
                    new CheckpointObservation(
                            options,
                            startedNanos,
                            ((StreamFusionKeyedStateBackend<?>) keyedStateBackend).usesNativeIncrementalCheckpoints()));
        }
        return startedNanos;
    }

    void snapshotSucceeded(long checkpointId, CheckpointOptions options, long startedNanos) {
        if (writeRawKeyedSnapshot) {
            statefulMetrics.checkpointCompleted(options, rawSnapshotBytes, -1, 0, System.nanoTime() - startedNanos);
        }
    }

    void snapshotFailed(long checkpointId) {
        fileCheckpoints.remove(checkpointId);
        statefulMetrics.checkpointFailed();
    }

    void finishSnapshotAttempt() {
        writeRawKeyedSnapshot = true;
    }

    void writeRawSnapshot(StateSnapshotContext context) throws Exception {
        if (!writeRawKeyedSnapshot) {
            return;
        }
        KeyedStateCheckpointOutputStream output = context.getRawKeyedOperatorStateOutput();
        DataOutputStream framedOutput = new DataOutputStream(output);
        for (int keyGroup : keyGroupRange) {
            output.startNewKeyGroup(keyGroup);
            byte[] state = bridge.snapshot(nativeHandle, keyGroup);
            framedOutput.writeInt(state.length);
            framedOutput.write(state);
            rawSnapshotBytes += Integer.BYTES + state.length;
        }
    }

    Path prepareIncrementalCheckpoint(long checkpointId) {
        if (rocksDbDirectory == null) {
            throw new IllegalStateException("Only native RocksDB state supports native file checkpoints");
        }
        Path checkpointDirectory = rocksDbDirectory.resolveSibling(
                "streamfusion-rocks-checkpoint-" + checkpointId + "-" + java.util.UUID.randomUUID());
        bridge.checkpointRocks(nativeHandle, checkpointDirectory);
        return checkpointDirectory;
    }

    void completeIncrementalCheckpoint(long checkpointId, long uploadedBytes, long reusedBytes) {
        CheckpointObservation observation = fileCheckpoints.remove(checkpointId);
        if (observation != null) {
            statefulMetrics.checkpointCompleted(
                    observation.options,
                    uploadedBytes + reusedBytes,
                    observation.incremental ? uploadedBytes : -1,
                    observation.incremental ? reusedBytes : 0,
                    System.nanoTime() - observation.startedNanos);
        }
    }

    void failIncrementalCheckpoint(long checkpointId) {
        if (fileCheckpoints.remove(checkpointId) != null) {
            statefulMetrics.checkpointFailed();
        }
    }

    void restoreIncrementalCheckpoint(Path checkpointDirectory, KeyGroupRange restoredRange) {
        long restoreStarted = System.nanoTime();
        long restoredBytes;
        try {
            restoredBytes = directorySize(checkpointDirectory);
        } catch (IOException failure) {
            statefulMetrics.restoreFailed();
            throw new IllegalStateException("Could not measure native RocksDB restore", failure);
        }
        long restoreReaderMemory = 256L * 1024;
        if (!managedMemory.tryReserve(restoreReaderMemory)) {
            statefulMetrics.restoreFailed();
            throw new IllegalStateException(
                    "Flink denied " + restoreReaderMemory + " bytes for the native RocksDB restore reader");
        }
        try {
            bridge.importRocksCheckpoint(
                    nativeHandle,
                    checkpointDirectory,
                    restoredRange.getStartKeyGroup(),
                    restoredRange.getEndKeyGroup(),
                    restoreReaderMemory);
            statefulMetrics.restored(restoredBytes, System.nanoTime() - restoreStarted);
        } catch (Throwable failure) {
            statefulMetrics.restoreFailed();
            throw failure;
        } finally {
            managedMemory.release(restoreReaderMemory);
        }
    }

    void close(CheckedRunnable beforeClose) throws Exception {
        long handle = nativeHandle;
        try {
            try {
                beforeClose.run();
            } finally {
                nativeHandle = 0;
                if (handle != 0) {
                    bridge.destroy(handle);
                    // The embedded-runner fallback owns this explicit cache allowance. It is
                    // not a native allocation callback and must be returned after DB teardown.
                    // A separately weighted STATE_BACKEND lease is owned by the keyed backend.
                    if (rocksDbOperatorReservation != 0) {
                        managedMemory.release(rocksDbOperatorReservation);
                        rocksDbOperatorReservation = 0;
                    }
                }
            }
        } finally {
            try {
                if (managedMemory != null) {
                    managedMemory.close();
                    managedMemory = null;
                    allocator = null;
                }
            } finally {
                deleteDirectory(rocksDbDirectory);
                rocksDbDirectory = null;
            }
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new)) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static long directorySize(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            return paths.filter(Files::isRegularFile)
                    .mapToLong(path -> {
                        try {
                            return Files.size(path);
                        } catch (IOException failure) {
                            throw new DirectorySizeException(failure);
                        }
                    })
                    .sum();
        } catch (DirectorySizeException failure) {
            throw failure.ioException;
        }
    }

    @FunctionalInterface
    interface CheckedRunnable {
        void run() throws Exception;
    }

    private static final class CheckpointObservation {
        private final CheckpointOptions options;
        private final long startedNanos;

        private final boolean incremental;

        private CheckpointObservation(CheckpointOptions options, long startedNanos, boolean incremental) {
            this.incremental = incremental;
            this.options = options;
            this.startedNanos = startedNanos;
        }
    }

    private static final class DirectorySizeException extends RuntimeException {
        private final IOException ioException;

        private DirectorySizeException(IOException ioException) {
            super(ioException);
            this.ioException = ioException;
        }
    }
}
