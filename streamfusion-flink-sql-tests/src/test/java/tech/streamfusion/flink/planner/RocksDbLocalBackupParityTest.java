/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalSnapshotDirectoryProviderImpl;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class RocksDbLocalBackupParityTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void backupContainsNativeFilesAndRemoteRestorePreservesGeneratedFlinkChangelogs(
            boolean incremental, boolean unaligned) throws Exception {
        var provider = new LocalSnapshotDirectoryProviderImpl(
                temporary.resolve("backups").toFile(),
                new org.apache.flink.api.common.JobID(),
                new org.apache.flink.runtime.jobgraph.JobVertexID(),
                0);
        var local = new LocalRecoveryConfig(false, true, provider);
        var live = new ArrayList<GenericRowData>();
        try (var oracle = SharedAggregateFlinkOracle.create(true, 0, true);
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSnapshotFinalizer snapshot;
            try (var source = SharedAggregateRuntimeHarness.localBackup(backend(incremental), null, local)) {
                for (int seed = 0; seed < 3; seed++)
                    SharedAggregateCheckpointTest.compare(source, oracle, allocator, live, seed);
                var location = CheckpointStorageLocationReference.getDefault();
                var options = unaligned
                        ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                        : CheckpointOptions.alignedNoTimeout(CheckpointType.CHECKPOINT, location);
                snapshot = OperatorSnapshotFinalizer.create(
                        source.region().snapshotState(5, 5, options, new MemCheckpointStreamFactory(64 << 20)));
            }
            var remote = snapshot.getJobManagerOwnedState();
            var backup = snapshot.getTaskLocalState();
            try {
                assertThat(remote.getManagedKeyedState()).hasSize(1);
                assertThat(backup.getManagedKeyedState()).hasSize(1);
                assertThat(backup.getRawKeyedState()).isEmpty();
                var remoteHandle = (IncrementalRemoteKeyedStateHandle)
                        remote.getManagedKeyedState().iterator().next();
                var localHandle = (IncrementalLocalKeyedStateHandle)
                        backup.getManagedKeyedState().iterator().next();
                Path directory = localHandle.getDirectoryStateHandle().getDirectory();
                assertThat(directory.getParent())
                        .isEqualTo(
                                provider.subtaskSpecificCheckpointDirectory(5).toPath());
                assertThat(directory.resolve("node-3/CURRENT")).exists();
                long bytes = 0;
                try (var files = Files.walk(directory)) {
                    for (var file : files.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toList())) {
                        bytes += Files.size(file);
                        String relative = directory.relativize(file).toString();
                        var handles = new ArrayList<>(remoteHandle.getSharedState());
                        handles.addAll(remoteHandle.getPrivateState());
                        var handle = handles.stream()
                                .filter(h -> h.getLocalPath().equals(relative))
                                .findFirst();
                        if (Files.size(file) == 0) continue;
                        assertThat(handle).isPresent();
                        try (var input = handle.orElseThrow().getHandle().openInputStream()) {
                            assertThat(input.readAllBytes()).isEqualTo(Files.readAllBytes(file));
                        }
                    }
                }
                assertThat(localHandle.getCheckpointedSize()).isEqualTo(bytes);
                // Backend close removed the live DB, not the checkpoint hardlinks. Recovery still
                // selects remote handles; backup-only must not change that Flink selection.
                try (var target = SharedAggregateRuntimeHarness.localBackup(backend(incremental), remote, local)) {
                    for (int seed = 3; seed < 6; seed++)
                        SharedAggregateCheckpointTest.compare(target, oracle, allocator, live, seed);
                }
                backup.discardState();
                assertThat(directory).doesNotExist();
            } finally {
                backup.discardState();
                remote.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static StreamFusionStateBackend backend(boolean incremental) {
        var options = new Configuration();
        options.set(CheckpointingOptions.LOCAL_BACKUP_ENABLED, true);
        return new StreamFusionStateBackend(new EmbeddedRocksDBStateBackend(incremental), options);
    }
}
