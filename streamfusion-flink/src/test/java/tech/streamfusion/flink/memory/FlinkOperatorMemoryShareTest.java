/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class FlinkOperatorMemoryShareTest {
    @Test
    void serializedOriginalWeightsUseFlinkConfigurationAndRoundingWithoutChangingNativeBudget() throws Exception {
        try (var environment =
                new MockEnvironmentBuilder().setManagedMemorySize(64L << 20).build()) {
            for (boolean managedBackend : List.of(false, true))
                for (var cases : List.of(
                        Set.of(ManagedMemoryUseCase.OPERATOR),
                        Set.of(ManagedMemoryUseCase.OPERATOR, ManagedMemoryUseCase.STATE_BACKEND),
                        Set.of(
                                ManagedMemoryUseCase.OPERATOR,
                                ManagedMemoryUseCase.PYTHON,
                                ManagedMemoryUseCase.STATE_BACKEND))) {
                    environment
                            .getJobConfiguration()
                            .set(
                                    TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                    Map.of("OPERATOR", "7", "STATE_BACKEND", "2", "PYTHON", "3"));
                    var runtime = new StreamConfig(new Configuration());
                    runtime.setStateBackendUsesManagedMemory(managedBackend);
                    // Actual native topology differs in both weights and use-case membership.
                    for (var useCase : ManagedMemoryUseCase.values())
                        runtime.setManagedMemoryFractionOperatorOfUseCase(useCase, 0.9);
                    var before = runtime.getConfiguration().toMap();
                    var original = new StreamConfig(new Configuration());
                    original.setStateBackendUsesManagedMemory(managedBackend);
                    for (var useCase : cases) original.setManagedMemoryFractionOperatorOfUseCase(useCase, 0.0);
                    double originalFraction =
                            org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils.getFractionRoundedDown(
                                    3, 17);
                    original.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, originalFraction);
                    long expected = environment
                            .getMemoryManager()
                            .computeMemorySize(original.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                    ManagedMemoryUseCase.OPERATOR,
                                    environment.getJobConfiguration(),
                                    environment.getTaskManagerInfo().getConfiguration(),
                                    getClass().getClassLoader()));
                    var share = InstantiationUtil.clone(new FlinkOperatorMemoryShare(3, 17, cases));
                    assertThat(share.memoryBytes(environment, runtime)).isEqualTo(expected);
                    assertThat(runtime.getConfiguration().toMap()).isEqualTo(before);
                    assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
                }
        }
    }

    @Test
    void rejectsUnknownOrNonPositiveOriginalShares() {
        for (int[] weights : List.of(new int[] {0, 1}, new int[] {2, 1}, new int[] {1, -1}))
            assertThatThrownBy(() ->
                            new FlinkOperatorMemoryShare(weights[0], weights[1], Set.of(ManagedMemoryUseCase.OPERATOR)))
                    .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlinkOperatorMemoryShare(1, 1, Set.of(ManagedMemoryUseCase.STATE_BACKEND)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
