/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.state.NativeCheckpointTransferTestStorage.Handle;
import tech.streamfusion.flink.state.NativeCheckpointTransferTestStorage.ReadGate;

class NativeCheckpointParallelTransferTest {
    @TempDir
    Path root;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lateHandlesAreDiscardedOnceAfterEveryWorkerExits(boolean closeOwner) throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var factory = new NativeCheckpointTransferTestStorage();
        var failed = new AtomicInteger();
        Path staging = staging("cancel");
        try (var parent = new CloseableRegistry();
                var uploads = new CloseableRegistry();
                var transfers = new NativeRocksDbTransfers(pool::shutdownNow, pool, parent)) {
            var task = task(staging, factory, failed, uploads, transfers, Map.of());
            var worker = new Thread(task, "checkpoint-cancel-coordinator");
            worker.start();
            try {
                await(factory.finalized);
                if (closeOwner) parent.close();
                else assertThat(task.cancel(false)).isTrue();
                assertThat(staging).isDirectory();
                assertThat(factory.handles).hasSize(2).allSatisfy(handle -> assertThat(handle.discards.get())
                        .isZero());
            } finally {
                factory.release.countDown();
                worker.join(5000);
            }
            assertThat(worker.isAlive()).isFalse();
            assertThat(task.isCancelled()).isTrue();
            assertThat(staging).doesNotExist();
            assertThat(factory.handles).hasSize(2).allSatisfy(handle -> assertThat(handle.discards.get())
                    .isEqualTo(1));
            assertThat(failed.get()).isEqualTo(1);
        } finally {
            factory.release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void parallelFileTransfersPreserveBytesNamespacesAndSstReuse() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var caller = Executors.newSingleThreadExecutor();
        var factory = new NativeCheckpointTransferTestStorage();
        try (var parent = new CloseableRegistry();
                var uploads = new CloseableRegistry();
                var transfers = new NativeRocksDbTransfers(pool::shutdownNow, pool, parent)) {
            Path source = staging("source");
            var task = task(source, factory, new AtomicInteger(), uploads, transfers, Map.of());
            caller.execute(task);
            await(factory.finalized);
            assertThat(task.isDone()).isFalse();
            factory.release.countDown();
            var handle = (IncrementalRemoteKeyedStateHandle)
                    task.get(5, TimeUnit.SECONDS).getJobManagerOwnedSnapshot();
            assertThat(factory.peak.get()).isEqualTo(2);
            assertThat(source).doesNotExist();
            assertThat(handle.getSharedState()).hasSize(4);
            assertThat(handle.getPrivateState()).hasSize(4);
            var reuse = new java.util.HashMap<String, NativeCheckpointUpload.SharedFile>();
            for (var file : handle.getSharedState())
                reuse.put(
                        file.getLocalPath(),
                        new NativeCheckpointUpload.SharedFile(
                                file.getHandle(), file.getHandle().getStateSize()));
            var second = task(staging("second"), factory, new AtomicInteger(), uploads, transfers, reuse);
            caller.execute(second);
            var reused = (IncrementalRemoteKeyedStateHandle)
                    second.get(5, TimeUnit.SECONDS).getJobManagerOwnedSnapshot();
            for (int i = 0; i < 4; i++)
                assertThat(reused.getSharedState().get(i).getHandle())
                        .isSameAs(handle.getSharedState().get(i).getHandle());
            assertThat(factory.reused.get()).isEqualTo(4);

            factory.readGate = new ReadGate();
            var download = caller.submit(
                    () -> NativeCheckpointDownload.materialize(handle, root.resolve("restore"), transfers));
            await(factory.readGate.entered);
            assertThat(download.isDone()).isFalse();
            factory.readGate.release.countDown();
            Path restored = download.get(5, TimeUnit.SECONDS);
            for (int i = 0; i < 8; i++)
                assertThat(Files.readAllBytes(restored.resolve(name(i)))).containsExactly(payload(i));
            assertThat(Files.size(restored.resolve("node-2/empty"))).isZero();
            org.apache.flink.util.FileUtils.deleteDirectory(restored.toFile());
            assertThat(factory.handles)
                    .allSatisfy(file -> assertThat(file.discards.get()).isZero());
        } finally {
            factory.release.countDown();
            if (factory.readGate != null) factory.readGate.release.countDown();
            pool.shutdownNow();
            caller.shutdownNow();
        }
    }

    @Test
    void checkpointSizeUsesStorageHandleSizesIncludingEncodingOverhead() throws Exception {
        var factory = new NativeCheckpointTransferTestStorage();
        factory.reportedOverhead = 19;
        factory.release.countDown();
        try (var uploads = new CloseableRegistry()) {
            var task = task(staging("sized"), factory, new AtomicInteger(), uploads, null, Map.of());
            task.run();
            var handle = (IncrementalRemoteKeyedStateHandle) task.get().getJobManagerOwnedSnapshot();
            long expected =
                    factory.handles.stream().mapToLong(Handle::getStateSize).sum();
            assertThat(handle.getCheckpointedSize()).isEqualTo(expected);
            assertThat(handle.getStateSize()).isEqualTo(expected);
        }
    }

    @Test
    void cancelledParallelDownloadsRemoveOnlyTheirStagingAndPreserveRemoteHandles() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var caller = Executors.newSingleThreadExecutor();
        var factory = new NativeCheckpointTransferTestStorage();
        factory.release.countDown();
        try (var parent = new CloseableRegistry();
                var uploads = new CloseableRegistry();
                var transfers = new NativeRocksDbTransfers(pool::shutdownNow, pool, parent)) {
            var task = task(staging("upload"), factory, new AtomicInteger(), uploads, transfers, Map.of());
            caller.execute(task);
            var handle = (IncrementalRemoteKeyedStateHandle)
                    task.get(5, TimeUnit.SECONDS).getJobManagerOwnedSnapshot();
            var destination = Files.createDirectory(root.resolve("restore"));
            Files.writeString(destination.resolve("neighbor"), "user data");
            factory.readGate = new ReadGate();
            var download = caller.submit(() -> NativeCheckpointDownload.materialize(handle, destination, transfers));
            await(factory.readGate.entered);
            parent.close();
            assertThatThrownBy(() -> download.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            try (var files = Files.list(destination)) {
                assertThat(files.map(Path::getFileName).collect(java.util.stream.Collectors.toList()))
                        .containsExactly(Path.of("neighbor"));
            }
            assertThat(factory.handles)
                    .allSatisfy(file -> assertThat(file.discards.get()).isZero());
        } finally {
            factory.release.countDown();
            if (factory.readGate != null) factory.readGate.release.countDown();
            pool.shutdownNow();
            caller.shutdownNow();
        }
    }

    private NativeCheckpointUploadTask task(
            Path staging,
            NativeCheckpointTransferTestStorage factory,
            AtomicInteger failed,
            CloseableRegistry uploads,
            NativeRocksDbTransfers transfers,
            Map<String, NativeCheckpointUpload.SharedFile> previous)
            throws Exception {
        var resources = new NativeCheckpointUploadResources(staging, factory, failed::incrementAndGet);
        return new NativeCheckpointUploadTask(
                () -> SnapshotResult.of(NativeCheckpointUpload.upload(
                                UUID.randomUUID(),
                                new KeyGroupRange(0, 0),
                                1,
                                true,
                                previous,
                                staging,
                                resources,
                                transfers)
                        .handle),
                resources,
                uploads,
                transfers);
    }

    private Path staging(String label) throws Exception {
        var directory = Files.createDirectory(root.resolve(label));
        for (int i = 0; i < 8; i++) {
            var file = directory.resolve(name(i));
            Files.createDirectories(file.getParent());
            Files.write(file, payload(i));
        }
        Files.createFile(directory.resolve("node-2/empty"));
        return directory;
    }

    private static String name(int i) {
        return "node-" + (2 + i % 2) + "/" + i + (i < 4 ? ".sst" : ".meta");
    }

    private static byte[] payload(int i) {
        byte[] bytes = new byte[16384];
        java.util.Arrays.fill(bytes, (byte) i);
        return bytes;
    }

    private static void await(CountDownLatch latch) throws Exception {
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
