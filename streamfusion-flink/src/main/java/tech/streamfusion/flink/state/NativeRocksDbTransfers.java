/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.StateBackend;

/** Uses Flink's executor selection and ownership for native checkpoint file I/O. */
final class NativeRocksDbTransfers implements Closeable {
    private final Closeable helper;
    private final ExecutorService executor;
    private final CloseableRegistry operations = new CloseableRegistry();
    private final CloseableRegistry owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    static String validatedConfigurationKey(org.apache.flink.configuration.ReadableConfig config)
            throws ReflectiveOperationException {
        var option = (org.apache.flink.configuration.ConfigOption<?>) Class.forName(
                        "org.apache.flink.state.rocksdb.RocksDBOptions",
                        false,
                        Thread.currentThread().getContextClassLoader())
                .getField("CHECKPOINT_TRANSFER_THREAD_NUM")
                .get(null);
        config.get(option); // Preserve Flink's integer parsing; zero and negative configured values are valid.
        return option.key();
    }

    static NativeRocksDbTransfers open(StateBackend backend, Environment environment, CloseableRegistry owner)
            throws ReflectiveOperationException, IOException {
        int threads = (Integer)
                backend.getClass().getMethod("getNumberOfTransferThreads").invoke(backend);
        var type = Class.forName(
                "org.apache.flink.state.rocksdb.RocksDBStateDataTransferHelper",
                false,
                Thread.currentThread().getContextClassLoader());
        var helper = (Closeable) type.getMethod("forThreadNumIfSpecified", int.class, ExecutorService.class)
                .invoke(
                        null,
                        threads,
                        org.apache.flink.util.MdcUtils.scopeToJob(
                                environment.getJobID(),
                                environment.getIOManager().getExecutorService()));
        final ExecutorService executor;
        try {
            executor = (ExecutorService) type.getMethod("getExecutorService").invoke(helper);
        } catch (ReflectiveOperationException | RuntimeException failure) {
            try {
                helper.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        return new NativeRocksDbTransfers(helper, executor, owner);
    }

    NativeRocksDbTransfers(Closeable helper, ExecutorService executor, CloseableRegistry owner) throws IOException {
        this.helper = helper;
        this.executor = executor;
        this.owner = owner;
        owner.registerCloseable(this);
    }

    void register(Closeable operation) throws IOException {
        operations.registerCloseable(operation);
    }

    void unregister(Closeable operation) {
        operations.unregisterCloseable(operation);
    }

    <T> List<T> run(List<? extends Callable<T>> work, Runnable cancelIO) throws Exception {
        var batch = new NativeCheckpointTransferBatch<T>(work, cancelIO);
        register(batch);
        try {
            return batch.run(executor);
        } finally {
            unregister(batch);
        }
    }

    static <T> List<T> execute(NativeRocksDbTransfers transfers, List<? extends Callable<T>> work, Runnable cancelIO)
            throws Exception {
        return transfers == null
                ? new NativeCheckpointTransferBatch<T>(work, cancelIO).run(Runnable::run)
                : transfers.run(work, cancelIO);
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;
        owner.unregisterCloseable(this);
        try {
            operations.close();
        } finally {
            // Flink's helper closes dedicated pools but leaves a borrowed TM pool running.
            helper.close();
        }
    }
}
