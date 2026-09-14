/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.state.rocksdb.RocksDBStateDataTransferHelper;
import org.junit.jupiter.api.Test;

class NativeRocksDbTransfersTest {
    @Test
    void configuredSelectionAndOwnershipMatchTheActualFlinkHelper() throws Exception {
        try (var environment = new MockEnvironmentBuilder().build()) {
            var shared = environment.getIOManager().getExecutorService();
            for (int configured : new int[] {-2, -1, 0, 1, 2, 4}) {
                var config = new Configuration();
                config.set(RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM, configured);
                config.set(org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND, "rocksdb");
                assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                        .isNull();
                var backend = new EmbeddedRocksDBStateBackend(true)
                        .configure(config, getClass().getClassLoader());
                int resolved = backend.getNumberOfTransferThreads();
                assertThat(resolved).isEqualTo(configured == -1 ? 4 : configured);
                try (var owner = new CloseableRegistry();
                        var oracle = RocksDBStateDataTransferHelper.forThreadNumIfSpecified(
                                resolved, org.apache.flink.util.MdcUtils.scopeToJob(environment.getJobID(), shared))) {
                    var nativeTransfers = NativeRocksDbTransfers.open(backend, environment, owner);
                    var field = NativeRocksDbTransfers.class.getDeclaredField("executor");
                    field.setAccessible(true);
                    var executor = (ExecutorService) field.get(nativeTransfers);
                    assertThat(executor.getClass())
                            .isEqualTo(oracle.getExecutorService().getClass());
                    if (executor instanceof java.util.concurrent.ThreadPoolExecutor) {
                        assertThat(((java.util.concurrent.ThreadPoolExecutor) executor).getMaximumPoolSize())
                                .isEqualTo(resolved);
                    }
                    var thread = Thread.currentThread();
                    var observed = nativeTransfers.run(List.of(() -> Thread.currentThread()), () -> {});
                    if (resolved == 0 || resolved == 1) assertThat(observed).containsExactly(thread);
                    else if (resolved > 1) assertThat(observed).doesNotContain(thread);
                    owner.close();
                    nativeTransfers.close();
                    assertThat(executor.isShutdown()).isEqualTo(resolved >= 0);
                    assertThat(shared.submit(() -> 42).get(5, TimeUnit.SECONDS)).isEqualTo(42);
                }
            }
        }
    }

    @Test
    void parallelWorkersPreserveResultOrderAndRespectExecutorConcurrency() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        var caller = Executors.newSingleThreadExecutor();
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var active = new AtomicInteger();
        var peak = new AtomicInteger();
        List<Callable<Integer>> work = IntStream.range(0, 12)
                .mapToObj(index -> (Callable<Integer>) () -> {
                    peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                    entered.countDown();
                    try {
                        await(release);
                        return index;
                    } finally {
                        active.decrementAndGet();
                    }
                })
                .collect(Collectors.toList());
        try {
            var batch = new NativeCheckpointTransferBatch<>(work, release::countDown);
            var result = caller.submit(() -> batch.run(executor));
            await(entered);
            assertThat(result.isDone()).isFalse();
            release.countDown();
            assertThat(result.get(5, TimeUnit.SECONDS))
                    .containsExactlyElementsOf(IntStream.range(0, 12).boxed().collect(Collectors.toList()));
            assertThat(peak.get()).isEqualTo(2);
            assertThat(active.get()).isZero();
        } finally {
            release.countDown();
            executor.shutdownNow();
            caller.shutdownNow();
        }
    }

    @Test
    void cancellationJoinsRunningWorkersEvenWhenShutdownRemovesQueuedTransfers() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        var caller = Executors.newSingleThreadExecutor();
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var exited = new AtomicInteger();
        List<Callable<Integer>> work = IntStream.range(0, 8)
                .mapToObj(index -> (Callable<Integer>) () -> {
                    entered.countDown();
                    boolean interrupted = false;
                    while (true) {
                        try {
                            release.await();
                            break;
                        } catch (InterruptedException ignored) {
                            interrupted = true;
                        }
                    }
                    if (interrupted) Thread.currentThread().interrupt();
                    exited.incrementAndGet();
                    return index;
                })
                .collect(Collectors.toList());
        try {
            var batch = new NativeCheckpointTransferBatch<>(work, () -> {});
            var result = caller.submit(() -> batch.run(executor));
            await(entered);
            batch.close();
            executor.shutdownNow();
            assertThat(result.isDone()).isFalse();
            release.countDown();
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(java.util.concurrent.CancellationException.class);
            assertThat(exited.get()).isEqualTo(2);
        } finally {
            release.countDown();
            executor.shutdownNow();
            caller.shutdownNow();
        }
    }

    @Test
    void rejectionAndWorkerFailureCancelSiblingsAndWaitForTheirExit() throws Exception {
        for (boolean reject : List.of(false, true)) {
            var executor = Executors.newFixedThreadPool(2);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var exited = new AtomicInteger();
            var submitted = new AtomicInteger();
            var batch = new NativeCheckpointTransferBatch<Integer>(
                    List.of(
                            () -> {
                                entered.countDown();
                                await(release);
                                exited.incrementAndGet();
                                return 1;
                            },
                            () -> {
                                await(entered);
                                throw new IOException("worker failure");
                            }),
                    release::countDown);
            try {
                assertThatThrownBy(() -> batch.run(task -> {
                            if (reject && submitted.getAndIncrement() > 0) {
                                await(entered);
                                throw new java.util.concurrent.RejectedExecutionException("rejected");
                            }
                            executor.execute(task);
                        }))
                        .hasMessage(reject ? "rejected" : "worker failure");
                assertThat(exited.get()).isEqualTo(1);
            } finally {
                release.countDown();
                executor.shutdownNow();
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }
}
