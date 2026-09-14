/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeCheckpointLocalRestoreTest {
    @TempDir
    Path root;

    @Test
    void linksOnlyImmutableSstsAndNeverConsumesTheRetainedSource() throws Exception {
        var source = source();
        try (var cancel = new CloseableRegistry()) {
            var target = NativeCheckpointLocalRestore.materialize(
                    handle(source, metadata(List.of("node-3/empty"))), root.resolve("restore"), cancel);
            assertThat(Files.isSameFile(source.resolve("node-3/1.sst"), target.resolve("node-3/1.sst")))
                    .isTrue();
            assertThat(Files.isSameFile(source.resolve("node-3/CURRENT"), target.resolve("node-3/CURRENT")))
                    .isFalse();
            Files.writeString(target.resolve("node-3/CURRENT"), "changed");
            assertThat(Files.readString(source.resolve("node-3/CURRENT"))).isEqualTo("manifest");
            org.apache.flink.util.FileUtils.deleteDirectory(source.toFile());
            assertThat(Files.readAllBytes(target.resolve("node-3/1.sst"))).containsExactly(1, 2, 3);
            org.apache.flink.util.FileUtils.deleteDirectory(target.toFile());
        }
    }

    @Test
    void crossFilesystemSstRestoreFallsBackToARealCopy() throws Exception {
        Path sharedMemory = Path.of("/dev/shm");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                Files.isWritable(sharedMemory) && !Files.getFileStore(root).equals(Files.getFileStore(sharedMemory)));
        Path destination = Files.createTempDirectory(sharedMemory, "streamfusion-local-restore-test-");
        var source = source();
        try (var cancel = new CloseableRegistry()) {
            var target =
                    NativeCheckpointLocalRestore.materialize(handle(source, metadata(List.of())), destination, cancel);
            assertThat(Files.isSameFile(source.resolve("node-3/1.sst"), target.resolve("node-3/1.sst")))
                    .isFalse();
            assertThat(Files.readAllBytes(target.resolve("node-3/1.sst"))).containsExactly(1, 2, 3);
        } finally {
            org.apache.flink.util.FileUtils.deleteDirectory(destination.toFile());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void malformedLocalNamespacesFailWithoutDeletingSourceOrNeighbors(int kind) throws Exception {
        var source = source();
        List<String> empty = List.of("node-3/empty", "node-3/empty");
        if (kind == 1) empty = List.of("../neighbor");
        if (kind == 2) {
            empty = List.of();
            Files.createSymbolicLink(source.resolve("node-3/link"), root.resolve("neighbor"));
        }
        Files.writeString(root.resolve("neighbor"), "user data");
        var handle = handle(source, metadata(empty));
        try (var cancel = new CloseableRegistry()) {
            assertThatThrownBy(() -> NativeCheckpointLocalRestore.materialize(handle, root.resolve("restore"), cancel))
                    .isInstanceOf(java.io.IOException.class);
            try (var children = Files.list(root.resolve("restore"))) {
                assertThat(children).isEmpty();
            }
            assertThat(source.resolve("node-3/CURRENT")).exists();
            assertThat(Files.readString(root.resolve("neighbor"))).isEqualTo("user data");
        }
    }

    @Test
    void cancellationClosesLocalMetadataAndCleansOnlyThePrivateRestoreDirectory() throws Exception {
        var source = source();
        var metadata = new NativeCheckpointBlockingHandle(NativeCheckpointMetadata.encode(List.of()), 1);
        var handle = handle(source, metadata);
        var worker = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var cancel = new CloseableRegistry()) {
            var result = worker.submit(
                    () -> NativeCheckpointLocalRestore.materialize(handle, root.resolve("restore"), cancel));
            try {
                assertThat(metadata.entered.await(5, TimeUnit.SECONDS)).isTrue();
                cancel.close();
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class);
                try (var children = Files.list(root.resolve("restore"))) {
                    assertThat(children).isEmpty();
                }
                assertThat(source.resolve("node-3/1.sst")).exists();
            } finally {
                metadata.release.countDown();
            }
        } finally {
            worker.shutdownNow();
        }
    }

    private Path source() throws Exception {
        Path source = Files.createDirectories(root.resolve("source/node-3")).getParent();
        Files.write(source.resolve("node-3/1.sst"), new byte[] {1, 2, 3});
        Files.writeString(source.resolve("node-3/CURRENT"), "manifest");
        Files.createFile(source.resolve("node-3/empty"));
        return source;
    }

    private static StreamStateHandle metadata(List<String> empty) throws Exception {
        return new ByteStreamStateHandle("metadata", NativeCheckpointMetadata.encode(empty));
    }

    private static IncrementalLocalKeyedStateHandle handle(Path directory, StreamStateHandle metadata)
            throws Exception {
        return new IncrementalLocalKeyedStateHandle(
                UUID.randomUUID(),
                1,
                DirectoryStateHandle.forPathWithSize(directory),
                new KeyGroupRange(0, 0),
                metadata,
                List.of());
    }
}
