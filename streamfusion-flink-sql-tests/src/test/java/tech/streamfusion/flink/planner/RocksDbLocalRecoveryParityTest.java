/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalSnapshotDirectoryProviderImpl;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.state.StreamFusionKeyedStateBackend;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class RocksDbLocalRecoveryParityTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void flinkSelectsLocalStateAndRetriesRemoteForMissingOrCorruptBackup(boolean incremental, boolean unaligned)
            throws Exception {
        for (int failure = 0; failure < 6; failure++) {
            var provider = new LocalSnapshotDirectoryProviderImpl(
                    temporary.resolve("backup-" + failure).toFile(),
                    new org.apache.flink.api.common.JobID(),
                    new org.apache.flink.runtime.jobgraph.JobVertexID(),
                    0);
            var local = LocalRecoveryConfig.backupAndRecoveryEnabled(provider);
            var live = new ArrayList<GenericRowData>();
            try (var oracle = SharedAggregateFlinkOracle.create(true, 0, true);
                    var allocator = new RootAllocator(64L << 20)) {
                OperatorSnapshotFinalizer snapshot;
                try (var source = SharedAggregateRuntimeHarness.localBackup(
                        new Backend(incremental, temporary.resolve("source-" + failure), 1), null, local)) {
                    for (int seed = 0; seed < 3; seed++)
                        SharedAggregateCheckpointTest.compare(source, oracle, allocator, live, seed);
                    var location = CheckpointStorageLocationReference.getDefault();
                    var options = unaligned
                            ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                            : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                    snapshot = OperatorSnapshotFinalizer.create(
                            source.region().snapshotState(7, 7, options, new MemCheckpointStreamFactory(64 << 20)));
                }
                var remote = snapshot.getJobManagerOwnedState();
                var backup = snapshot.getTaskLocalState();
                var localHandle = (IncrementalLocalKeyedStateHandle)
                        backup.getManagedKeyedState().iterator().next();
                var directory = localHandle.getDirectoryStateHandle().getDirectory();
                try {
                    damage(localHandle, failure);
                    var targetRoot = temporary.resolve("target-" + failure);
                    var backend = new Backend(incremental, targetRoot, 3);
                    try (var target = SharedAggregateRuntimeHarness.localRecovery(
                            backend, failure == 0 ? forbidRemoteFileReads(remote) : remote, backup, local)) {
                        assertThat(backend.attempts)
                                .containsExactlyElementsOf(failure == 0 ? List.of(true) : List.of(true, false));
                        assertThat(backend.failures).isEqualTo(failure == 0 ? 0 : 1);
                        // Failed candidates must not leave stale registrations under the reused operator ID.
                        var flinkMetrics = oracle.getOperator().getMetricGroup();
                        flinkMetrics.gauge(
                                "currentInputWatermark",
                                new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                        flinkMetrics.gauge(
                                "currentOutputWatermark",
                                new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                        SharedAggregateMetricSurfaceTest.compare(
                                SharedAggregateMetricSurfaceTest.metrics(flinkMetrics),
                                SharedAggregateMetricSurfaceTest.metrics(
                                        SharedAggregateMetricSurfaceTest.stageGroup(target, 3)));
                        try (var files = Files.walk(targetRoot)) {
                            var options = files.filter(path ->
                                            path.getFileName().toString().startsWith("OPTIONS-"))
                                    .collect(java.util.stream.Collectors.toList());
                            assertThat(options).isNotEmpty();
                            for (var option : options)
                                assertThat(Files.readString(option)).contains("max_background_jobs=3");
                        }
                        for (int seed = 3; seed < 6; seed++)
                            SharedAggregateCheckpointTest.compare(target, oracle, allocator, live, seed);
                        // A restored local backend must remain able to create subsequent checkpoints.
                        var location = CheckpointStorageLocationReference.getDefault();
                        var options = unaligned
                                ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                                : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                        var next = OperatorSnapshotFinalizer.create(
                                target.region().snapshotState(8, 8, options, new MemCheckpointStreamFactory(64 << 20)));
                        next.getTaskLocalState().discardState();
                        next.getJobManagerOwnedState().discardState();
                    }
                    if (failure != 1) assertThat(directory).exists(); // Restore never consumes Flink's retained backup.
                } finally {
                    backup.discardState();
                    remote.discardState();
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    private static void damage(IncrementalLocalKeyedStateHandle handle, int failure) throws Exception {
        Path directory = handle.getDirectoryStateHandle().getDirectory();
        switch (failure) {
            case 0:
                return;
            case 1:
                org.apache.flink.util.FileUtils.deleteDirectory(directory.toFile());
                return;
            case 2:
                Files.writeString(directory.resolve("node-3/CURRENT"), "invalid-manifest\n");
                return;
            case 3:
                Files.delete(Path.of(handle.getMetaDataStateHandle()
                        .maybeGetPath()
                        .orElseThrow()
                        .toUri()));
                return;
            case 4:
                try (var files = Files.walk(directory)) {
                    var file = files.filter(path -> path.toString().endsWith(".sst"))
                            .findFirst()
                            .orElseThrow();
                    var bytes = Files.readAllBytes(file);
                    bytes[0] ^= 0x7f;
                    Files.write(file, bytes);
                }
                return;
            case 5:
                Files.writeString(
                        Path.of(handle.getMetaDataStateHandle()
                                .maybeGetPath()
                                .orElseThrow()
                                .toUri()),
                        "SFI1");
                return;
            default:
                throw new AssertionError(failure);
        }
    }

    static OperatorSubtaskState forbidRemoteFileReads(OperatorSubtaskState snapshot) {
        var original = (IncrementalRemoteKeyedStateHandle)
                snapshot.getManagedKeyedState().iterator().next();
        var remote = new IncrementalRemoteKeyedStateHandle(
                original.getBackendIdentifier(),
                original.getKeyGroupRange(),
                original.getCheckpointId(),
                forbid(original.getSharedState()),
                forbid(original.getPrivateState()),
                original.getMetaDataStateHandle(),
                original.getCheckpointedSize());
        return snapshot.toBuilder()
                .setManagedKeyedState(StateObjectCollection.singleton(remote))
                .build();
    }

    private static List<HandleAndLocalPath> forbid(List<HandleAndLocalPath> files) {
        return files.stream()
                .map(file -> HandleAndLocalPath.of(
                        new ByteStreamStateHandle("forbidden", new byte[0]) {
                            @Override
                            public org.apache.flink.core.fs.FSDataInputStream openInputStream() throws IOException {
                                throw new IOException("Healthy local recovery must not download remote files");
                            }

                            @Override
                            public long getStateSize() {
                                return file.getHandle().getStateSize();
                            }
                        },
                        file.getLocalPath()))
                .collect(java.util.stream.Collectors.toList());
    }

    static final class Backend implements StateBackend {
        final StreamFusionStateBackend delegate;
        final List<Boolean> attempts = new ArrayList<>();
        int failures;

        Backend(boolean incremental) {
            this(incremental, null, 2);
        }

        Backend(boolean incremental, Path root, int backgroundJobs) {
            var options = new Configuration();
            options.set(StateRecoveryOptions.LOCAL_RECOVERY, true);
            if (root != null)
                options.set(org.apache.flink.state.rocksdb.RocksDBOptions.LOCAL_DIRECTORIES, root.toString());
            options.set(
                    org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, backgroundJobs);
            delegate = new StreamFusionStateBackend(
                    new EmbeddedRocksDBStateBackend(incremental)
                            .configure(options, getClass().getClassLoader()),
                    options);
        }

        @Override
        public boolean useManagedMemory() {
            return delegate.useManagedMemory();
        }

        @Override
        public org.apache.flink.runtime.state.OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            return delegate.createOperatorStateBackend(parameters);
        }

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
                throws Exception {
            boolean restored = !parameters.getStateHandles().isEmpty();
            if (restored)
                attempts.add(
                        parameters.getStateHandles().iterator().next() instanceof IncrementalLocalKeyedStateHandle);
            try {
                var backend = delegate.createKeyedStateBackend(parameters);
                if (restored)
                    assertThat(((StreamFusionKeyedStateBackend<?>) backend).usesNativeFileCheckpoints())
                            .isTrue();
                return backend;
            } catch (Exception | Error failure) {
                failures++;
                assertThat(parameters.getEnv().getMemoryManager().verifyEmpty()).isTrue();
                throw failure;
            }
        }
    }
}
