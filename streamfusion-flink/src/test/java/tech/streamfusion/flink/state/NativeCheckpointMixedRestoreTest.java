/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Flink can prioritize a candidate containing local and remote handles for different ranges. */
class NativeCheckpointMixedRestoreTest {
    @TempDir
    Path root;

    @Test
    void mixedCandidateRestrictsRangesAndRetainsOriginalHandles() throws Exception {
        var local = local(new KeyGroupRange(0, 3));
        var remote = remote(new KeyGroupRange(4, 7));
        var skipped = local(new KeyGroupRange(8, 15));
        // A handle outside the assignment must not be materialized, even if no longer available.
        org.apache.flink.util.FileUtils.deleteDirectory(
                skipped.getDirectoryStateHandle().getDirectory().toFile());
        try (var cancellation = new CloseableRegistry();
                var prepared = NativeCheckpointRestore.prepare(
                        List.of(local, remote, skipped),
                        new KeyGroupRange(2, 5),
                        root.resolve("target"),
                        null,
                        cancellation)) {
            var ranges = new ArrayList<KeyGroupRange>();
            prepared.restore(new NativeIncrementalStateParticipant() {
                @Override
                public Path prepareIncrementalCheckpoint(long id) {
                    throw new AssertionError();
                }

                @Override
                public void restoreIncrementalCheckpoint(Path directory, KeyGroupRange range) throws Exception {
                    ranges.add(range);
                    assertThat(Files.readAllBytes(directory.resolve("node-3/1.sst")))
                            .containsExactly(1, 2, 3);
                }
            });
            assertThat(ranges).containsExactly(new KeyGroupRange(2, 3), new KeyGroupRange(4, 5));
            try (var files = Files.list(root.resolve("target"))) {
                assertThat(files).isEmpty();
            }
            assertThat(local.getDirectoryStateHandle().getDirectory().resolve("node-3/1.sst"))
                    .exists();
            assertThat(remote.getPrivateState().get(0).getHandle().getStateSize())
                    .isEqualTo(3);
        }
    }

    @Test
    void failedLaterLocalHandleCleansEarlierRemoteStagingOnly() throws Exception {
        var local = local(new KeyGroupRange(4, 7));
        Files.delete(local.getDirectoryStateHandle().getDirectory().resolve("node-3/empty"));
        var remote = remote(new KeyGroupRange(0, 3));
        try (var cancellation = new CloseableRegistry()) {
            assertThatThrownBy(() -> NativeCheckpointRestore.prepare(
                            List.of(remote, local),
                            new KeyGroupRange(0, 7),
                            root.resolve("target"),
                            null,
                            cancellation))
                    .isInstanceOf(java.io.IOException.class);
            try (var files = Files.list(root.resolve("target"))) {
                assertThat(files).isEmpty();
            }
            assertThat(local.getDirectoryStateHandle().getDirectory().resolve("node-3/1.sst"))
                    .exists();
            assertThat(remote.getPrivateState().get(0).getHandle().getStateSize())
                    .isEqualTo(3);
        }
    }

    private IncrementalLocalKeyedStateHandle local(KeyGroupRange range) throws Exception {
        var directory = Files.createTempDirectory(root, "local-");
        Files.createDirectories(directory.resolve("node-3"));
        Files.write(directory.resolve("node-3/1.sst"), new byte[] {1, 2, 3});
        Files.createFile(directory.resolve("node-3/empty"));
        return new IncrementalLocalKeyedStateHandle(
                UUID.randomUUID(),
                7,
                DirectoryStateHandle.forPathWithSize(directory),
                range,
                new ByteStreamStateHandle("local-metadata", NativeCheckpointMetadata.encode(List.of("node-3/empty"))),
                List.of());
    }

    private static IncrementalRemoteKeyedStateHandle remote(KeyGroupRange range) throws Exception {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.randomUUID(),
                range,
                7,
                List.of(),
                List.of(HandleAndLocalPath.of(
                        new ByteStreamStateHandle("remote-sst", new byte[] {1, 2, 3}), "node-3/1.sst")),
                new ByteStreamStateHandle("remote-metadata", NativeCheckpointMetadata.encode(List.of())),
                3);
    }
}
