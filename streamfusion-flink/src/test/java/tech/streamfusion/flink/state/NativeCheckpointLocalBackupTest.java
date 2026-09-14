/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeCheckpointLocalBackupTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publishedBackupSurvivesCloseAndDiscardDoesNotOwnRemoteSsts(boolean incremental) throws Exception {
        var participant = new NativeCheckpointUploadCancellationTest.Participant(temporary);
        var factory = new NativeCheckpointUploadCancellationTest.Factory();
        SnapshotResult<KeyedStateHandle> snapshot;
        var config = NativeCheckpointUploadCancellationTest.localConfig(temporary);
        try (var backend = NativeCheckpointUploadCancellationTest.backend(participant, incremental, config)) {
            var future = backend.snapshot(5, 5, factory, CheckpointOptions.forCheckpointWithDefaultLocation());
            future.run();
            snapshot = future.get();
            assertThat(future.cancel(false)).isFalse();
            backend.notifyCheckpointComplete(5);
        }
        var remote = (IncrementalRemoteKeyedStateHandle) snapshot.getJobManagerOwnedSnapshot();
        var local = (IncrementalLocalKeyedStateHandle) snapshot.getTaskLocalSnapshot();
        assertThat(local.getBackendIdentifier()).isEqualTo(remote.getBackendIdentifier());
        assertThat(local.getCheckpointId()).isEqualTo(5);
        assertThat(local.getKeyGroupRange()).isEqualTo(remote.getKeyGroupRange());
        assertThat(local.getSharedStateHandles()).isEqualTo(remote.getSharedState());
        assertThat(NativeCheckpointMetadata.isNativeHandle(local)).isTrue();
        Path directory = local.getDirectoryStateHandle().getDirectory();
        assertThat(directory).isEqualTo(participant.staging);
        assertThat(directory.getParent().getFileName().toString()).isEqualTo("chk_5");
        assertThat(Files.readAllBytes(directory.resolve("000001.sst"))).containsExactly(1, 2, 3);
        assertThat(local.getCheckpointedSize()).isEqualTo(5);
        assertThat(local.getStateSize())
                .isEqualTo(5 + remote.getMetaDataStateHandle().getStateSize());
        var metadata = local.getMetaDataStateHandle().maybeGetPath().orElseThrow();
        assertThat(Path.of(metadata.getParent().toUri())).isEqualTo(directory.getParent());
        var store = new org.apache.flink.runtime.state.TaskLocalStateStoreImpl(
                new org.apache.flink.api.common.JobID(),
                new org.apache.flink.runtime.clusterframework.types.AllocationID(),
                new org.apache.flink.runtime.jobgraph.JobVertexID(),
                0,
                config,
                Runnable::run);
        try {
            var taskSnapshot = new org.apache.flink.runtime.checkpoint.TaskStateSnapshot();
            taskSnapshot.putSubtaskStateByOperatorID(
                    new org.apache.flink.runtime.jobgraph.OperatorID(),
                    org.apache.flink.runtime.checkpoint.OperatorSubtaskState.builder()
                            .setManagedKeyedState(
                                    org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton(local))
                            .build());
            store.storeLocalState(5, taskSnapshot);
            store.confirmCheckpoint(5);
            assertThat(directory).exists();
            assertThat(store.retrieveLocalState(5)).isNull(); // Backup-only does not select local restore.
            store.abortCheckpoint(5);
        } finally {
            store.dispose().get();
        }
        assertThat(Files.exists(directory)).isFalse();
        assertThat(Files.exists(Path.of(metadata.toUri()))).isFalse();
        assertThat(factory.handles)
                .allSatisfy(handle -> assertThat(handle.discards.get()).isZero());
        remote.discardState();
    }

    @Test
    void cancellationAfterMetadataFinalizationDiscardsLocalAndRemoteBeforePublication() throws Exception {
        var config = NativeCheckpointUploadCancellationTest.localConfig(temporary);
        var directory = NativeCheckpointLocalBackup.prepare(config, UUID.randomUUID(), 9);
        Files.createDirectory(directory);
        Files.write(directory.resolve("CURRENT"), new byte[] {1});
        var factory = new NativeCheckpointUploadCancellationTest.Factory();
        var resources = new NativeCheckpointUploadResources(directory, factory, () -> {}, config, 9);
        var uploaded = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var owner = new org.apache.flink.core.fs.CloseableRegistry()) {
            var task = new NativeCheckpointUploadTask(
                    () -> {
                        var result = NativeCheckpointUpload.upload(
                                UUID.randomUUID(),
                                new KeyGroupRange(0, 0),
                                9,
                                true,
                                java.util.Map.of(),
                                directory,
                                resources,
                                null);
                        uploaded.countDown();
                        if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS))
                            throw new java.io.IOException("timeout");
                        return result.snapshotResult();
                    },
                    resources,
                    owner);
            var worker = new Thread(task);
            worker.start();
            try {
                assertThat(uploaded.await(5, java.util.concurrent.TimeUnit.SECONDS))
                        .isTrue();
                assertThat(task.cancel(false)).isTrue();
            } finally {
                release.countDown();
                worker.join(5000);
            }
            assertThat(worker.isAlive()).isFalse();
            assertThat(Files.exists(directory)).isFalse();
            try (var children = Files.list(directory.getParent())) {
                assertThat(children).isEmpty();
            }
            assertThat(factory.handles)
                    .allSatisfy(handle -> assertThat(handle.discards.get()).isOne());
        }
    }

    @Test
    void localMetadataFailureKeepsRemoteCheckpointAndRemovesUnusableBackup() throws Exception {
        Path directory = Files.createDirectory(temporary.resolve("db"));
        Files.write(directory.resolve("CURRENT"), new byte[] {1});
        Path blocker = Files.write(temporary.resolve("not-a-directory"), new byte[] {0});
        var provider =
                new org.apache.flink.runtime.state.LocalSnapshotDirectoryProviderImpl(
                        temporary.toFile(),
                        new org.apache.flink.api.common.JobID(),
                        new org.apache.flink.runtime.jobgraph.JobVertexID(),
                        0) {
                    @Override
                    public java.io.File subtaskSpecificCheckpointDirectory(long id) {
                        return blocker.toFile();
                    }
                };
        var config = new org.apache.flink.runtime.state.LocalRecoveryConfig(false, true, provider);
        var factory = new NativeCheckpointUploadCancellationTest.Factory();
        NativeCheckpointUpload.Result result;
        try (var resources = new NativeCheckpointUploadResources(
                directory,
                factory,
                () -> {
                    throw new AssertionError("A local metadata failure must not fail the remote checkpoint");
                },
                config,
                1)) {
            result = NativeCheckpointUpload.upload(
                    UUID.randomUUID(),
                    new KeyGroupRange(0, 0),
                    1,
                    true,
                    java.util.Map.of(),
                    directory,
                    resources,
                    null);
            resources.publish();
        }
        assertThat(result.snapshotResult().getTaskLocalSnapshot()).isNull();
        assertThat(result.handle).isNotNull();
        assertThat(directory).doesNotExist();
        assertThat(blocker).exists();
        assertThat(factory.handles)
                .allSatisfy(handle -> assertThat(handle.discards.get()).isZero());
        result.handle.discardState();
    }

    @Test
    void namespaceCleanupNeverDeletesAnotherChainedOperatorsBackup() throws Exception {
        var config = NativeCheckpointUploadCancellationTest.localConfig(temporary);
        UUID firstId = UUID.randomUUID();
        var first = NativeCheckpointLocalBackup.prepare(config, firstId, 3);
        var second = NativeCheckpointLocalBackup.prepare(config, UUID.randomUUID(), 3);
        Files.createDirectory(first);
        Files.createDirectory(second);
        Files.write(first.resolve("stale"), new byte[] {1});
        Files.write(second.resolve("owned"), new byte[] {2});
        assertThat(NativeCheckpointLocalBackup.prepare(config, firstId, 3)).isEqualTo(first);
        assertThat(Files.exists(first)).isFalse();
        assertThat(Files.readAllBytes(second.resolve("owned"))).containsExactly(2);
    }

    @Test
    void backupAndRecoveryIncludingDeprecatedAliasAreAdmitted() {
        var config = new Configuration();
        config.set(CheckpointingOptions.LOCAL_BACKUP_ENABLED, true);
        assertThat(NativeStateConfigurationSupport.rocksDbUnsupportedReason(config))
                .isNull();
        for (String key : List.of(StateRecoveryOptions.LOCAL_RECOVERY.key(), "state.backend.local-recovery")) {
            var recovery = new Configuration(config);
            recovery.setString(key, "true");
            assertThat(NativeStateConfigurationSupport.rocksDbUnsupportedReason(recovery))
                    .isNull();
        }
    }
}
