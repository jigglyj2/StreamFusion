/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Joins actual worker exits before checkpoint staging or unpublished handles can be cleaned. */
final class NativeCheckpointTransferBatch<T> implements Closeable {
    private final List<Transfer> work = new ArrayList<>();
    private final Runnable cancelIO;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();

    NativeCheckpointTransferBatch(List<? extends Callable<T>> work, Runnable cancelIO) {
        this.cancelIO = cancelIO;
        for (var operation : work) this.work.add(new Transfer(operation));
    }

    List<T> run(Executor executor) throws Exception {
        try {
            for (var transfer : work) {
                if (cancelled.get()) break;
                executor.execute(transfer);
            }
        } catch (Throwable rejected) {
            fail(rejected);
        }
        boolean interrupted = false;
        for (var transfer : work) {
            while (true) {
                try {
                    transfer.exited.await();
                    break;
                } catch (InterruptedException interruption) {
                    interrupted = true;
                    fail(interruption);
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        Throwable problem = failure.get();
        if (problem instanceof Error) throw (Error) problem;
        if (problem instanceof Exception) throw (Exception) problem;
        var results = new ArrayList<T>(work.size());
        for (var transfer : work) results.add(transfer.get());
        return results;
    }

    private void fail(Throwable problem) {
        failure.compareAndSet(null, problem);
        close();
    }

    @Override
    public void close() {
        failure.compareAndSet(null, new CancellationException("Native checkpoint file transfer cancelled"));
        if (!cancelled.compareAndSet(false, true)) return;
        try {
            cancelIO.run();
        } catch (Throwable cleanup) {
            Throwable problem = failure.get();
            if (problem != cleanup) problem.addSuppressed(cleanup);
        } finally {
            for (var transfer : work) transfer.cancel(false);
        }
    }

    private final class Transfer extends FutureTask<T> {
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final CountDownLatch exited = new CountDownLatch(1);

        Transfer(Callable<T> operation) {
            super(() -> {
                try {
                    return operation.call();
                } catch (Exception | Error problem) {
                    fail(problem);
                    throw problem;
                }
            });
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) return;
            try {
                super.run();
            } finally {
                exited.countDown();
            }
        }

        @Override
        public boolean cancel(boolean interrupt) {
            boolean result = super.cancel(interrupt);
            // Queued tasks may be removed by shutdownNow; they must not keep the join alive.
            if (result && claimed.compareAndSet(false, true)) exited.countDown();
            return result;
        }
    }
}
