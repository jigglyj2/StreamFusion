/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns only this upload's staging files, active streams and newly created remote handles. */
final class NativeCheckpointUploadResources implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NativeCheckpointUploadResources.class);
    private final Path directory;
    private final CheckpointStreamFactory factory;
    private final Runnable failure;
    private final CloseableRegistry io = new CloseableRegistry();
    private final java.util.Queue<StreamStateHandle> created = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final AtomicBoolean cleaned = new AtomicBoolean();
    private Runnable publication = () -> {};
    private boolean published;
    private boolean localHandleCreated;
    private final LocalRecoveryConfig localRecovery;
    private final long checkpointId;

    NativeCheckpointUploadResources(Path directory, CheckpointStreamFactory factory, Runnable failure) {
        this(directory, factory, failure, LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED, -1);
    }

    NativeCheckpointUploadResources(
            Path directory,
            CheckpointStreamFactory factory,
            Runnable failure,
            LocalRecoveryConfig localRecovery,
            long checkpointId) {
        this.localRecovery = localRecovery;
        this.checkpointId = checkpointId;
        this.directory = directory;
        this.factory = factory;
        this.failure = failure;
    }

    void checkCancelled() {
        if (io.isClosed()) throw new CancellationException("Native checkpoint upload was cancelled");
    }

    void reuse(StreamStateHandle handle) throws IOException {
        checkCancelled();
        factory.reusePreviousStateHandle(List.of(handle));
    }

    boolean canReuse(StreamStateHandle handle) {
        checkCancelled();
        return factory.couldReuseStateHandle(handle);
    }

    StreamStateHandle upload(Path file, CheckpointedStateScope scope) throws IOException {
        checkCancelled();
        try (InputStream input = Files.newInputStream(file)) {
            io.registerCloseable(input);
            try {
                return upload(scope, input::transferTo);
            } finally {
                io.unregisterCloseable(input);
            }
        }
    }

    StreamStateHandle upload(byte[] bytes, CheckpointedStateScope scope) throws IOException {
        return upload(scope, output -> output.write(bytes));
    }

    private StreamStateHandle upload(CheckpointedStateScope scope, Writer writer) throws IOException {
        checkCancelled();
        CheckpointStateOutputStream output = factory.createCheckpointStateOutputStream(scope);
        io.registerCloseable(output);
        boolean finalized = false;
        try {
            writer.write(output);
            checkCancelled();
            StreamStateHandle handle = output.closeAndGetHandle();
            if (handle == null) throw new IOException("Checkpoint upload returned no state handle");
            created.add(handle);
            finalized = true;
            checkCancelled();
            return handle;
        } finally {
            if (io.unregisterCloseable(output) && !finalized) output.close();
        }
    }

    SnapshotResult<StreamStateHandle> uploadMetadata(byte[] bytes) throws IOException {
        checkCancelled();
        var provider = localRecovery.isLocalBackupEnabled()
                ? CheckpointStreamWithResultProvider.createDuplicatingStream(
                        checkpointId,
                        CheckpointedStateScope.EXCLUSIVE,
                        factory,
                        localRecovery
                                .getLocalStateDirectoryProvider()
                                .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled()))
                : CheckpointStreamWithResultProvider.createSimpleStream(CheckpointedStateScope.EXCLUSIVE, factory);
        io.registerCloseable(provider);
        boolean finalized = false;
        try {
            provider.getCheckpointOutputStream().write(bytes);
            checkCancelled();
            var result = provider.closeAndFinalizeCheckpointStreamResult();
            if (result.getJobManagerOwnedSnapshot() != null) created.add(result.getJobManagerOwnedSnapshot());
            if (result.getTaskLocalSnapshot() != null) created.add(result.getTaskLocalSnapshot());
            finalized = true;
            checkCancelled();
            if (result.getJobManagerOwnedSnapshot() == null) {
                throw new IOException("Checkpoint metadata upload returned no state handle");
            }
            return result;
        } finally {
            if (io.unregisterCloseable(provider) && !finalized) provider.close();
        }
    }

    DirectoryStateHandle localDirectoryHandle() throws IOException {
        var handle = DirectoryStateHandle.forPathWithSize(directory);
        localHandleCreated = true;
        return handle;
    }

    void onPublication(Runnable action) {
        publication = action;
    }

    // Called only while the future excludes cancellation. Until then, every created handle
    // remains owned here, including when cancellation races the last closeAndGetHandle().
    void publish() {
        checkCancelled();
        publication.run();
        published = true;
    }

    void cancelIO() {
        try {
            io.close();
        } catch (IOException failure) {
            LOG.warn("Could not close native checkpoint I/O", failure);
        }
    }

    @Override
    public void close() {
        if (!cleaned.compareAndSet(false, true)) return;
        cancelIO();
        if (!published) {
            for (StreamStateHandle handle : created) {
                try {
                    handle.discardState();
                } catch (Exception failure) {
                    LOG.warn("Could not discard unpublished native checkpoint state", failure);
                }
            }
            try {
                failure.run();
            } catch (RuntimeException failure) {
                LOG.warn("Could not report native checkpoint failure", failure);
            }
        }
        created.clear();
        if (published && localHandleCreated) return;
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
                Files.deleteIfExists(path);
        } catch (IOException failure) {
            LOG.warn("Could not remove native checkpoint staging {}", directory, failure);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(CheckpointStateOutputStream output) throws IOException;
    }
}
