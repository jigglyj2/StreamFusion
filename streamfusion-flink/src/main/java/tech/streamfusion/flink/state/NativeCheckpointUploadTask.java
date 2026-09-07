/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

/** Flink-style snapshot resource ownership, with atomic cancellation/result publication. */
final class NativeCheckpointUploadTask extends FutureTask<SnapshotResult<KeyedStateHandle>> {
    private final Object publicationLock = new Object();
    private final AtomicBoolean cleanupClaimed = new AtomicBoolean();
    private final NativeCheckpointUploadResources resources;
    private final CloseableRegistry owner;
    private final Closeable cancelOnClose = () -> cancel(true);

    NativeCheckpointUploadTask(
            Callable<SnapshotResult<KeyedStateHandle>> upload,
            NativeCheckpointUploadResources resources,
            CloseableRegistry owner)
            throws IOException {
        super(upload);
        this.resources = resources;
        this.owner = owner;
        owner.registerCloseable(cancelOnClose);
    }

    @Override
    public void run() {
        // Like AsyncSnapshotCallable, the worker or pre-start cancellation owns cleanup, never both.
        if (!cleanupClaimed.compareAndSet(false, true)) return;
        try {
            super.run();
        } finally {
            resources.close();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled;
        synchronized (publicationLock) {
            cancelled = super.cancel(mayInterruptIfRunning);
        }
        if (cancelled) {
            resources.cancelIO();
            if (cleanupClaimed.compareAndSet(false, true)) resources.close();
        }
        return cancelled;
    }

    @Override
    protected void set(SnapshotResult<KeyedStateHandle> result) {
        synchronized (publicationLock) {
            if (isCancelled()) return;
            try {
                resources.publish();
                resources.close();
                super.set(result);
            } catch (Throwable failure) {
                resources.close();
                super.setException(failure);
            }
        }
    }

    @Override
    protected void setException(Throwable failure) {
        synchronized (publicationLock) {
            resources.close();
            super.setException(failure);
        }
    }

    @Override
    protected void done() {
        owner.unregisterCloseable(cancelOnClose);
    }
}
