/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.memory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.api.Test;

class SharedNativeMemoryTest {
    @Test
    void weightedOperatorsBorrowBeforePeersOpenWithoutChangingTheirOriginalShares() throws Exception {
        try (var environment =
                new MockEnvironmentBuilder().setManagedMemorySize(1L << 20).build()) {
            var small = config(0.125, false);
            var before = small.getConfiguration().toMap();
            try (var busy = create(environment, small, "busy")) {
                long limit = busy.limit();
                assertThat(busy.assignedOperatorShare()).isLessThan(limit / 2);
                assertThat(busy.tryReserve(limit * 3 / 4)).isTrue();
                // Opening order and a peer's weight do not partition the shared capacity.
                try (var idle = create(environment, config(0.875, false), "idle")) {
                    assertThat(idle.limit()).isEqualTo(limit);
                    assertThat(idle.available()).isEqualTo(limit / 4);
                    assertThat(idle.tryReserve(limit / 4)).isTrue();
                    assertThat(busy.tryReserve(1)).isFalse();
                    busy.release(limit * 3 / 4);
                    assertThat(idle.tryReserve(limit * 3 / 4)).isTrue();
                    idle.release(limit);
                }
            }
            assertThat(small.getConfiguration().toMap()).isEqualTo(before);
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    @Test
    void sharedOperatorCeilingLeavesOtherUseCaseBudgetsAvailable() throws Exception {
        try (var environment =
                new MockEnvironmentBuilder().setManagedMemorySize(8L << 20).build()) {
            var config = config(0.125, true);
            config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.PYTHON, 0);
            var full = new StreamConfig(new Configuration(config.getConfiguration()));
            full.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1);
            var manager = environment.getMemoryManager();
            long expected = manager.computeMemorySize(full.getManagedMemoryFractionOperatorUseCaseOfSlot(
                    ManagedMemoryUseCase.OPERATOR,
                    environment.getJobConfiguration(),
                    environment.getTaskManagerInfo().getConfiguration(),
                    getClass().getClassLoader()));
            try (var first = create(environment, config, "first");
                    var second = create(environment, config, "second")) {
                assertThat(first.limit()).isEqualTo(expected).isLessThan(manager.getMemorySize());
                assertThat(first.tryReserve(expected)).isTrue();
                assertThat(second.tryReserve(1)).isFalse();
                Object backend = new Object();
                long remainder = manager.getMemorySize() - expected;
                manager.reserveMemory(backend, remainder);
                assertThat(manager.availableMemory()).isZero();
                first.release(expected);
                assertThat(second.tryReserve(expected)).isTrue();
                second.release(expected);
                manager.releaseMemory(backend, remainder);
            }
            assertThat(manager.verifyEmpty()).isTrue();
        }
    }

    @Test
    void externalFlinkReservationsAndSeparateSlotsRemainIndependent() throws Exception {
        MemoryManager one = MemoryManager.create(1L << 20, 32 << 10);
        MemoryManager two = MemoryManager.create(1L << 20, 32 << 10);
        Object flink = new Object();
        one.reserveMemory(flink, 1L << 19);
        try (var first = new FlinkManagedMemory(one, 1L << 20, "one");
                var second = new FlinkManagedMemory(two, 1L << 20, "two")) {
            assertThat(first.available()).isEqualTo(1L << 19);
            assertThat(first.tryReserve((1L << 19) + 1)).isFalse();
            assertThat(second.tryReserve(1L << 20)).isTrue();
            assertThat(first.tryReserve(1L << 19)).isTrue();
            first.release(1L << 19);
            second.release(1L << 20);
        }
        one.releaseMemory(flink, 1L << 19);
        assertThat(one.verifyEmpty()).isTrue();
        assertThat(two.verifyEmpty()).isTrue();
    }

    @Test
    void retainedClosedOwnerKeepsPoolAliveUntilItsFinalRelease() {
        MemoryManager manager = MemoryManager.create(1L << 20, 32 << 10);
        var first = new FlinkManagedMemory(manager, 4096, "first");
        assertThat(first.tryReserve(3072)).isTrue();
        first.close();
        try (var second = new FlinkManagedMemory(manager, 4096, "second")) {
            assertThat(second.available()).isEqualTo(1024);
            first.release(3072);
            assertThat(second.tryReserve(4096)).isTrue();
            second.release(4096);
        }
        assertThat(manager.verifyEmpty()).isTrue();
        // All leases ended: a fresh resource can initialize with a different configured size.
        try (var replacement = new FlinkManagedMemory(manager, 8192, "replacement")) {
            assertThat(replacement.tryReserve(8192)).isTrue();
            replacement.release(8192);
        }
        assertThat(manager.verifyEmpty()).isTrue();
    }

    @Test
    void concurrentOperatorsCannotBothConsumeTheSameFreeCapacity() throws Exception {
        MemoryManager manager = MemoryManager.create(1L << 20, 32 << 10);
        var executor = Executors.newFixedThreadPool(2);
        try (var first = new FlinkManagedMemory(manager, 4096, "first");
                var second = new FlinkManagedMemory(manager, 4096, "second")) {
            var ready = new CountDownLatch(2);
            var start = new CountDownLatch(1);
            java.util.function.Function<FlinkManagedMemory, Callable<Boolean>> attempt = memory -> () -> {
                ready.countDown();
                start.await();
                return memory.tryReserve(3072);
            };
            var a = executor.submit(attempt.apply(first));
            var b = executor.submit(attempt.apply(second));
            ready.await();
            start.countDown();
            boolean left = a.get();
            boolean right = b.get();
            assertThat(left ^ right).isTrue();
            (left ? first : second).release(3072);
        } finally {
            executor.shutdownNow();
        }
        assertThat(manager.verifyEmpty()).isTrue();
    }

    private static StreamConfig config(double fraction, boolean managedBackend) {
        var config = new StreamConfig(new Configuration());
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, fraction);
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 0);
        config.setStateBackendUsesManagedMemory(managedBackend);
        return config;
    }

    private static FlinkManagedMemory create(MockEnvironment environment, StreamConfig config, String name) {
        return FlinkManagedMemory.create(
                environment, config, UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(), name);
    }
}
