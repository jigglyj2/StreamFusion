/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BackendRestorerProcedure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;

class NativeCheckpointRestoreCreationTest {
    @TempDir
    Path root;

    @ParameterizedTest
    @ValueSource(ints = {0, 2, -2})
    void preparationOutlivesRestoreRegistryAndTransfersRemainUsableForCheckpointing(int threads) throws Exception {
        try (var fixture = new Fixture(threads)) {
            var bytes = new ByteStreamStateHandle("files", new byte[] {1, 2, 3});
            var backend = fixture.create(List.of(handle(bytes)));
            try {
                fixture.cancellation.close(); // Flink does this before operator initialization.
                assertThat(stagingCount()).isOne();
                var imported = new AtomicInteger();
                backend.registerNativeStateParticipant(
                        new NativeIncrementalStateParticipant() {
                            @Override
                            public Path prepareIncrementalCheckpoint(long id) throws Exception {
                                var path = Files.createTempDirectory(root, "checkpoint-");
                                Files.write(path.resolve("CURRENT"), new byte[] {4});
                                return path;
                            }

                            @Override
                            public void restoreIncrementalCheckpoint(Path path, KeyGroupRange range) throws Exception {
                                assertThat(Files.readAllBytes(path.resolve("node-3/CURRENT")))
                                        .containsExactly(1, 2, 3);
                                assertThat(range).isEqualTo(new KeyGroupRange(0, 0));
                                imported.incrementAndGet();
                            }
                        },
                        true);
                assertThat(imported.get()).isOne();
                assertThat(stagingCount()).isZero();
                var snapshot = backend.snapshot(
                        9,
                        9,
                        new MemCheckpointStreamFactory(1024),
                        CheckpointOptions.forCheckpointWithDefaultLocation());
                snapshot.run();
                snapshot.get().discardState();
            } finally {
                backend.close();
                backend.dispose();
            }
        }
    }

    @Test
    void flinkRetriesFailedDownloadBeforeAnyParticipantIsRegistered() throws Exception {
        try (var fixture = new Fixture(2);
                var lifetime = new CloseableRegistry()) {
            var good = handle(new ByteStreamStateHandle("good", new byte[] {1}));
            var bad = handle(new org.apache.flink.runtime.state.filesystem.FileStateHandle(
                    new org.apache.flink.core.fs.Path(root.resolve("missing").toUri()), 1));
            var attempts = new AtomicInteger();
            var restorer = new BackendRestorerProcedure<StreamFusionKeyedStateBackend<Integer>, KeyedStateHandle>(
                    handles -> {
                        assertThat(stagingCount()).isZero();
                        assertThat(fixture.env.getMemoryManager().verifyEmpty()).isTrue();
                        attempts.incrementAndGet();
                        return fixture.create(handles);
                    },
                    lifetime,
                    "native checkpoint download");
            var backend = restorer.createAndRestore(
                    List.of(List.of(bad), List.of(good)), StateObject.StateObjectSizeStatsCollector.create());
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(stagingCount()).isOne();
            backend.close(); // No participant: a later initialization failure must not leak staging.
            assertThatThrownBy(() -> backend.registerNativeStateParticipant(
                            new NativeIncrementalStateParticipant() {
                                @Override
                                public Path prepareIncrementalCheckpoint(long id) {
                                    throw new AssertionError();
                                }

                                @Override
                                public void restoreIncrementalCheckpoint(Path directory, KeyGroupRange range) {
                                    throw new AssertionError("A closed restore must not reach the participant");
                                }
                            },
                            true))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("closed before import");
            backend.dispose();
            assertThat(stagingCount()).isZero();
        }
    }

    @Test
    void failedLaterHandleCleansEarlierMaterializationAndReleasesMemory() throws Exception {
        try (var fixture = new Fixture(2)) {
            var good = handle(new ByteStreamStateHandle("good", new byte[] {1}));
            var bad = handle(new org.apache.flink.runtime.state.filesystem.FileStateHandle(
                    new org.apache.flink.core.fs.Path(root.resolve("missing").toUri()), 1));
            assertThatThrownBy(() -> fixture.create(List.of(good, bad))).isInstanceOf(IOException.class);
            assertThat(stagingCount()).isZero();
            assertThat(fixture.env.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void cancellationClosesIdentificationMetadataAndDataReadsDuringConstruction(int phase) throws Exception {
        var bytes = phase == 2 ? new byte[] {1} : NativeCheckpointMetadata.encode(List.of());
        var blocked = new NativeCheckpointBlockingHandle(bytes, phase == 1 ? 2 : 1);
        var metadata = phase == 2
                ? new ByteStreamStateHandle("metadata", NativeCheckpointMetadata.encode(List.of()))
                : blocked;
        var data = phase == 2 ? blocked : new ByteStreamStateHandle("data", new byte[] {1});
        var candidate = handle(data, metadata);
        var caller = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var fixture = new Fixture(2)) {
            var result = caller.submit(() -> fixture.create(List.of(candidate)));
            try {
                assertThat(blocked.entered.await(5, TimeUnit.SECONDS)).isTrue();
                fixture.cancellation.close();
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                assertThat(blocked.closed.get()).isPositive();
                assertThat(stagingCount()).isZero();
                assertThat(fixture.env.getMemoryManager().verifyEmpty()).isTrue();
            } finally {
                blocked.release.countDown();
            }
        } finally {
            caller.shutdownNow();
        }
    }

    private long stagingCount() throws IOException {
        if (!Files.exists(root)) return 0;
        try (var files = Files.walk(root)) {
            return files.filter(path -> path.getFileName().toString().startsWith("streamfusion-rocks-restore-"))
                    .count();
        }
    }

    private static IncrementalRemoteKeyedStateHandle handle(StreamStateHandle data) throws IOException {
        return handle(data, new ByteStreamStateHandle("metadata", NativeCheckpointMetadata.encode(List.of())));
    }

    private static IncrementalRemoteKeyedStateHandle handle(StreamStateHandle data, StreamStateHandle metadata) {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.randomUUID(),
                new KeyGroupRange(0, 0),
                1,
                List.of(),
                List.of(HandleAndLocalPath.of(data, "node-3/CURRENT")),
                metadata,
                data.getStateSize() + metadata.getStateSize());
    }

    private final class Fixture implements AutoCloseable {
        final MockEnvironment env =
                new MockEnvironmentBuilder().setManagedMemorySize(32L << 20).build();
        final CloseableRegistry cancellation = new CloseableRegistry();
        final StreamFusionStateBackend wrapper;
        final String identifier;

        Fixture(int threads) throws Exception {
            var options = new Configuration();
            options.set(org.apache.flink.state.rocksdb.RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM, threads);
            var delegate = new EmbeddedRocksDBStateBackend(true)
                    .configure(options, getClass().getClassLoader());
            delegate.setDbStoragePath(root.resolve("state").toString());
            wrapper = new StreamFusionStateBackend(delegate, options);
            var config = new StreamConfig(new Configuration());
            config.setOperatorID(new OperatorID());
            NativeStateOwnership.register(env, config, StreamFusionArrowNativeRegionOperator.class);
            identifier = new OperatorSubtaskDescriptionText(
                            config.getOperatorID(), StreamFusionArrowNativeRegionOperator.class.getSimpleName(), 0, 1)
                    .toString();
        }

        StreamFusionKeyedStateBackend<Integer> create(java.util.Collection<KeyedStateHandle> handles) throws Exception {
            return (StreamFusionKeyedStateBackend<Integer>)
                    wrapper.createKeyedStateBackend(new KeyedStateBackendParametersImpl<>(
                            env,
                            env.getJobID(),
                            identifier,
                            IntSerializer.INSTANCE,
                            1,
                            new KeyGroupRange(0, 0),
                            env.getTaskKvStateRegistry(),
                            TtlTimeProvider.DEFAULT,
                            env.getMetricGroup(),
                            (name, value) -> {},
                            handles,
                            cancellation,
                            0.5));
        }

        @Override
        public void close() throws Exception {
            cancellation.close();
            assertThat(env.getMemoryManager().verifyEmpty()).isTrue();
            env.close();
        }
    }
}
