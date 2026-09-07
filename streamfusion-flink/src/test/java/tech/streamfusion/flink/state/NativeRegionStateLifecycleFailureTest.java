/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativeRegionStateLifecycleFailureTest {
    @Test
    void failedConstructionAndRestoreReturnNativeAndFallbackCacheLeasesBeforeEnvironmentCloses() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (boolean duringRestore : List.of(false, true)) {
                try (var environment = new MockEnvironmentBuilder()
                        .setManagedMemorySize(64L << 20)
                        .build()) {
                    var config = new StreamConfig(new Configuration());
                    config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
                    var backend = (CheckpointableKeyedStateBackend<?>) Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {CheckpointableKeyedStateBackend.class},
                            (proxy, method, args) -> {
                                if (method.getName().equals("getKeyGroupRange")) return new KeyGroupRange(0, 15);
                                if (method.getName().equals("getBackendTypeIdentifier"))
                                    return rocks ? "rocksdb" : "hashmap";
                                throw new UnsupportedOperationException(method.toString());
                            });
                    var initialization = (StateInitializationContext) Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {StateInitializationContext.class},
                            (proxy, method, args) -> {
                                throw new IllegalStateException("injected restore failure");
                            });
                    var input = Operator.newBuilder()
                            .setPlanNodeId(1)
                            .setInput(Input.newBuilder())
                            .build();
                    var root = duringRestore
                            ? Operator.newBuilder()
                                    .setPlanNodeId(2)
                                    .setDeduplicate(Deduplicate.newBuilder()
                                            .setInput(input)
                                            .setProcessingTime(true)
                                            .setGenerateInsert(true)
                                            .addKeyIndices(0))
                                    .build()
                            : input;
                    byte[] plan = NativePlan.newBuilder()
                            .setProtocolVersion(2)
                            .setRoot(root)
                            .build()
                            .toByteArray();
                    var spill = environment
                            .getIOManager()
                            .getSpillingDirectories()[0]
                            .toPath();
                    long before;
                    try (var paths = Files.list(spill)) {
                        before = paths.count();
                    }
                    try (var lifecycle = new NativeRegionStateLifecycle()) {
                        assertThatThrownBy(() -> lifecycle.initialize(
                                        initialization,
                                        environment,
                                        config,
                                        UnregisteredMetricsGroup.createOperatorMetricGroup(),
                                        backend,
                                        16,
                                        plan,
                                        List.of(2L)))
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining(
                                        duringRestore ? "injected restore failure" : "no matching persistent node");
                        // Check now: MockEnvironment.close() would otherwise clear leaked reservations.
                        assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
                        try (var paths = Files.list(spill)) {
                            assertThat(paths.count()).isEqualTo(before);
                        }
                    }
                }
            }
    }
}
