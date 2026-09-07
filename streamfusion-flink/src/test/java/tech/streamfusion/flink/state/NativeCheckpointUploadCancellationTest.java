/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeCheckpointUploadCancellationTest {
    @Test
    void cancellationDuringStreamFinalizationDiscardsTheLateHandle(@TempDir Path directory) throws Exception {
        var owner = new Participant(directory);
        var factory = new Factory();
        factory.blockFinalizationAt = 1;
        try (var backend = backend(owner)) {
            var future = backend.snapshot(1, 1, factory, CheckpointOptions.forCheckpointWithDefaultLocation());
            Thread worker = new Thread(future, "native-checkpoint-finalization-test");
            worker.start();
            try {
                assertThat(factory.entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(future.cancel(false)).isTrue();
            } finally {
                factory.release.countDown();
                worker.join(5000);
            }
            assertThat(worker.isAlive()).isFalse();
            assertThat(owner.completed.get()).isZero();
            assertThat(owner.failed.get()).isOne();
            assertThat(factory.handles).hasSize(1).allSatisfy(handle -> assertThat(handle.discards.get())
                    .isOne());
            assertThat(Files.exists(owner.staging)).isFalse();
        }
    }

    @Test
    void closedBackendRejectsNewUploadsAndCleansTheirStaging(@TempDir Path directory) throws Exception {
        var owner = new Participant(directory);
        var factory = new Factory();
        var backend = backend(owner);
        backend.close();
        assertThatThrownBy(() -> backend.snapshot(1, 1, factory, CheckpointOptions.forCheckpointWithDefaultLocation()))
                .isInstanceOf(IOException.class);
        assertThat(owner.completed.get()).isZero();
        assertThat(owner.failed.get()).isOne();
        assertThat(factory.opened.get()).isZero();
        assertThat(Files.exists(owner.staging)).isFalse();
    }

    @Test
    void cancellationAfterUploadBeforePublicationDiscardsUnpublishedHandles(@TempDir Path directory) throws Exception {
        var factory = new Factory();
        var completed = new AtomicInteger();
        var failed = new AtomicInteger();
        var uploaded = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Path staging = Files.createTempDirectory(directory, "publication-");
        var resources = new NativeCheckpointUploadResources(staging, factory, failed::incrementAndGet);
        try (var owner = new org.apache.flink.core.fs.CloseableRegistry()) {
            var task = new NativeCheckpointUploadTask(
                    () -> {
                        StreamStateHandle handle = resources.upload(new byte[] {1}, CheckpointedStateScope.EXCLUSIVE);
                        resources.onPublication(completed::incrementAndGet);
                        uploaded.countDown();
                        if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("publication test timed out");
                        return org.apache.flink.runtime.state.SnapshotResult.of(
                                new org.apache.flink.runtime.state.KeyGroupsStateHandle(
                                        new org.apache.flink.runtime.state.KeyGroupRangeOffsets(
                                                new KeyGroupRange(0, 0)),
                                        handle));
                    },
                    resources,
                    owner);
            Thread worker = new Thread(task, "native-checkpoint-publication-test");
            worker.start();
            try {
                assertThat(uploaded.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(task.cancel(false)).isTrue();
            } finally {
                release.countDown();
                worker.join(5000);
            }
            assertThat(worker.isAlive()).isFalse();
            assertThat(completed.get()).isZero();
            assertThat(failed.get()).isOne();
            assertThat(factory.handles).hasSize(1).allSatisfy(handle -> assertThat(handle.discards.get())
                    .isOne());
            assertThat(Files.exists(staging)).isFalse();
        }
    }

    @Test
    void cancellationBeforeExecutionCleansStagingAndReportsFailureExactlyOnce(@TempDir Path directory)
            throws Exception {
        for (boolean interrupt : List.of(false, true)) {
            var owner = new Participant(directory);
            var factory = new Factory();
            try (var backend = backend(owner)) {
                var future = backend.snapshot(1, 1, factory, CheckpointOptions.forCheckpointWithDefaultLocation());
                assertThat(Files.exists(owner.staging)).isTrue();
                assertThat(future.cancel(interrupt)).isTrue();
                assertThat(future.cancel(interrupt)).isFalse();
                future.run();
                assertThat(Files.exists(owner.staging)).isFalse();
                assertThat(factory.opened.get()).isZero();
                assertThat(owner.completed.get()).isZero();
                assertThat(owner.failed.get()).isOne();
            }
        }
    }

    @Test
    void cancellationClosesBlockedIoAndDiscardsPreviouslyUploadedNewHandles(@TempDir Path directory) throws Exception {
        for (boolean interrupt : List.of(false, true)) {
            var owner = new Participant(directory);
            var factory = new Factory();
            factory.blockAt = 2;
            try (var backend = backend(owner)) {
                var future = backend.snapshot(2, 2, factory, CheckpointOptions.forCheckpointWithDefaultLocation());
                Thread worker = new Thread(future, "native-checkpoint-cancellation-test");
                worker.start();
                try {
                    assertThat(factory.entered.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(future.cancel(interrupt)).isTrue();
                } finally {
                    factory.release.countDown();
                    worker.join(5000);
                }
                assertThat(worker.isAlive()).isFalse();
                assertThat(owner.completed.get()).isZero();
                assertThat(owner.failed.get()).isOne();
                assertThat(factory.handles).hasSize(1).allSatisfy(handle -> assertThat(handle.discards.get())
                        .isOne());
                assertThat(factory.aborted.get()).isOne();
                assertThat(Files.exists(owner.staging)).isFalse();
            }
        }
    }

    @Test
    void failedUploadDiscardsNewStateButNeverReusedSsts(@TempDir Path directory) throws Exception {
        var owner = new Participant(directory);
        var first = new Factory();
        try (var backend = backend(owner)) {
            var complete = backend.snapshot(1, 1, first, CheckpointOptions.forCheckpointWithDefaultLocation());
            complete.run();
            complete.get();
            backend.notifyCheckpointComplete(1);
            assertThat(complete.cancel(true)).isFalse();
            assertThat(first.handles)
                    .allSatisfy(handle -> assertThat(handle.discards.get()).isZero());
            var failing = new Factory();
            failing.failAt = 2; // existing SST is reused, CURRENT uploaded, metadata upload fails.
            var failed = backend.snapshot(2, 2, failing, CheckpointOptions.forCheckpointWithDefaultLocation());
            failed.run();
            assertThatThrownBy(failed::get).hasCauseInstanceOf(IOException.class);
            assertThat(failing.reused.get()).isOne();
            assertThat(failing.handles).hasSize(1).allSatisfy(handle -> assertThat(handle.discards.get())
                    .isOne());
            assertThat(first.handles)
                    .allSatisfy(handle -> assertThat(handle.discards.get()).isZero());
            assertThat(owner.completed.get()).isOne();
            assertThat(owner.failed.get()).isOne();
            assertThat(Files.exists(owner.staging)).isFalse();
        }
    }

    @Test
    void backendCloseCancelsUnstartedUploadsAndStorageCanRefuseSstReuse(@TempDir Path directory) throws Exception {
        var owner = new Participant(directory);
        var factory = new Factory();
        var backend = backend(owner);
        var first = backend.snapshot(1, 1, factory, CheckpointOptions.forCheckpointWithDefaultLocation());
        first.run();
        first.get();
        backend.notifyCheckpointComplete(1);
        var refusing = new Factory();
        refusing.allowReuse = false;
        var second = backend.snapshot(2, 2, refusing, CheckpointOptions.forCheckpointWithDefaultLocation());
        second.run();
        second.get();
        assertThat(refusing.reused.get()).isZero();
        assertThat(refusing.handles).hasSize(factory.handles.size());
        var pending = backend.snapshot(3, 3, factory, CheckpointOptions.forCheckpointWithDefaultLocation());
        backend.close();
        assertThat(pending.isCancelled()).isTrue();
        assertThat(Files.exists(owner.staging)).isFalse();
        assertThat(owner.failed.get()).isOne();
    }

    private static StreamFusionKeyedStateBackend<?> backend(Participant owner) throws Exception {
        var delegate = (CheckpointableKeyedStateBackend<?>) Proxy.newProxyInstance(
                NativeCheckpointUploadCancellationTest.class.getClassLoader(),
                new Class<?>[] {CheckpointableKeyedStateBackend.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getKeyGroupRange")) return new KeyGroupRange(0, 0);
                    if (method.getName().equals("close") || method.getName().equals("dispose")) return null;
                    throw new UnsupportedOperationException(method.toString());
                });
        var result = new StreamFusionKeyedStateBackend<>(delegate, List.of(), "rocksdb", null, true);
        result.registerNativeStateParticipant(owner, true);
        return result;
    }

    private static final class Participant implements NativeIncrementalStateParticipant {
        final Path parent;
        Path staging;
        final AtomicInteger completed = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();

        Participant(Path parent) {
            this.parent = parent;
        }

        @Override
        public Path prepareIncrementalCheckpoint(long id) throws Exception {
            staging = Files.createTempDirectory(parent, "checkpoint-");
            Files.write(staging.resolve("000001.sst"), new byte[] {1, 2, 3});
            Files.write(staging.resolve("CURRENT"), new byte[] {4, 5});
            return staging;
        }

        @Override
        public void completeIncrementalCheckpoint(long id, long uploaded, long reused) {
            completed.incrementAndGet();
        }

        @Override
        public void failIncrementalCheckpoint(long id) {
            failed.incrementAndGet();
        }

        @Override
        public void restoreIncrementalCheckpoint(Path path, KeyGroupRange range) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class Handle extends ByteStreamStateHandle {
        final AtomicInteger discards = new AtomicInteger();

        Handle(byte[] bytes) {
            super(java.util.UUID.randomUUID().toString(), bytes);
        }

        @Override
        public void discardState() {
            discards.incrementAndGet();
        }
    }

    private static final class Factory extends MemCheckpointStreamFactory {
        final AtomicInteger opened = new AtomicInteger();
        final AtomicInteger aborted = new AtomicInteger();
        final AtomicInteger reused = new AtomicInteger();
        final List<Handle> handles = new ArrayList<>();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        int failAt = -1;
        int blockAt = -1;
        int blockFinalizationAt = -1;
        boolean allowReuse = true;

        Factory() {
            super(1 << 20);
        }

        @Override
        public boolean couldReuseStateHandle(StreamStateHandle handle) {
            return allowReuse;
        }

        @Override
        public void reusePreviousStateHandle(java.util.Collection<? extends StreamStateHandle> handles) {
            reused.addAndGet(handles.size());
        }

        @Override
        public CheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope) {
            int index = opened.incrementAndGet();
            return new CheckpointStateOutputStream() {
                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                volatile boolean closed;
                boolean finalized;

                @Override
                public void write(int value) throws IOException {
                    if (index == blockAt) {
                        entered.countDown();
                        try {
                            if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("test timed out");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException(interrupted);
                        }
                    }
                    if (closed || index == failAt) throw new IOException("injected checkpoint write failure");
                    bytes.write(value);
                }

                @Override
                public long getPos() {
                    return bytes.size();
                }

                @Override
                public void flush() {}

                @Override
                public void sync() {}

                @Override
                public StreamStateHandle closeAndGetHandle() throws IOException {
                    Handle handle;
                    synchronized (this) {
                        if (closed) throw new IOException("closed checkpoint stream");
                        handle = new Handle(bytes.toByteArray());
                        handles.add(handle);
                        finalized = true;
                        closed = true;
                    }
                    // Model storage which has finalized remote state but has not returned its handle yet.
                    if (index == blockFinalizationAt) {
                        entered.countDown();
                        try {
                            if (!release.await(5, TimeUnit.SECONDS))
                                throw new IOException("finalization test timed out");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException(interrupted);
                        }
                    }
                    return handle;
                }

                @Override
                public synchronized void close() {
                    if (!closed && !finalized) aborted.incrementAndGet();
                    closed = true;
                    release.countDown();
                }
            };
        }
    }
}
