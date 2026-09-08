/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
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
import tech.streamfusion.proto.plan.v1.*;

class SharedRegionLifecycleTest {
    @Test
    void sharedConstructionAndRestoreFailuresReleaseStateAndFlinkAllowancesOnBothBackends() throws Exception {
        var plan = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(1)
                .addOutputStageIds(2)
                .addOutputStageIds(4)
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(2)
                                .setDeduplicate(Deduplicate.newBuilder()
                                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                                        .setProcessingTime(true)
                                        .setGenerateInsert(true)
                                        .addKeyIndices(0)))
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0)))
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(4)
                                .setCalc(Calc.newBuilder()
                                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                                        .addProjections(Expression.newBuilder()
                                                .setInputReference(InputReference.newBuilder()
                                                        .setIndex(0)))
                                        .setPreserveInputEnvelope(true)))
                        .addInputs(NativeRegionInputReference.newBuilder().setStageId(2)))
                .build()
                .toByteArray();
        for (boolean rocks : List.of(false, true))
            for (boolean restore : List.of(false, true)) {
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
                    var spill = environment
                            .getIOManager()
                            .getSpillingDirectories()[0]
                            .toPath();
                    long before;
                    try (var paths = Files.list(spill)) {
                        before = paths.count();
                    }
                    try (var lifecycle = new NativeRegionStateLifecycle()) {
                        assertThatThrownBy(() -> lifecycle.initializeRegion(
                                        initialization,
                                        environment,
                                        config,
                                        UnregisteredMetricsGroup.createOperatorMetricGroup(),
                                        backend,
                                        16,
                                        plan,
                                        List.of(restore ? 2L : 3L),
                                        null))
                                .hasMessageContaining(
                                        restore ? "injected restore failure" : "no matching physical definition");
                        assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
                        try (var paths = Files.list(spill)) {
                            assertThat(paths.count()).isEqualTo(before);
                        }
                    }
                }
            }
    }
}
