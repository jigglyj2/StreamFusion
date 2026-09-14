/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.AbstractIncrementalStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.FileUtils;

/** Materializes a restore candidate inside Flink's backend-construction retry boundary. */
final class NativeCheckpointRestore implements Closeable {
    private final List<Entry> entries = new ArrayList<>();
    private boolean closed;

    static NativeCheckpointRestore prepare(
            List<? extends AbstractIncrementalStateHandle> handles,
            KeyGroupRange assigned,
            Path root,
            NativeRocksDbTransfers transfers,
            CloseableRegistry cancellation)
            throws Exception {
        var result = new NativeCheckpointRestore();
        try {
            if (transfers != null) cancellation.registerCloseable(transfers);
            for (var handle : handles) {
                if (cancellation.isClosed()) throw new IOException("Native checkpoint preparation cancelled");
                var range = assigned.getIntersection(handle.getKeyGroupRange());
                if (range.getNumberOfKeyGroups() == 0) continue;
                Path directory;
                if (handle instanceof IncrementalRemoteKeyedStateHandle) {
                    directory = NativeCheckpointDownload.materialize(
                            (IncrementalRemoteKeyedStateHandle) handle, root, transfers, cancellation);
                } else if (handle instanceof IncrementalLocalKeyedStateHandle) {
                    directory = NativeCheckpointLocalRestore.materialize(
                            (IncrementalLocalKeyedStateHandle) handle, root, cancellation);
                } else
                    throw new IOException("Unsupported native checkpoint handle "
                            + handle.getClass().getName());
                result.entries.add(new Entry(directory, range));
            }
            if (cancellation.isClosed()) throw new IOException("Native checkpoint preparation cancelled");
            return result;
        } catch (Exception | Error failure) {
            try {
                result.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        } finally {
            // Flink closes this restore-only registry after construction. The keyed backend
            // owns the still-live transfer helper for later checkpoints.
            if (transfers != null) cancellation.unregisterCloseable(transfers);
        }
    }

    synchronized void restore(NativeIncrementalStateParticipant participant) throws Exception {
        if (closed) throw new IOException("Native checkpoint preparation closed before import");
        try {
            for (var entry : entries) participant.restoreIncrementalCheckpoint(entry.directory, entry.range);
        } catch (Exception | Error failure) {
            try {
                close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        close();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        IOException failure = null;
        for (var entry : entries) {
            try {
                FileUtils.deleteDirectory(entry.directory.toFile());
            } catch (IOException cleanup) {
                if (failure == null) failure = cleanup;
                else failure.addSuppressed(cleanup);
            }
        }
        entries.clear();
        if (failure != null) throw failure;
    }

    private static final class Entry {
        final Path directory;
        final KeyGroupRange range;

        Entry(Path directory, KeyGroupRange range) {
            this.directory = directory;
            this.range = range;
        }
    }
}
