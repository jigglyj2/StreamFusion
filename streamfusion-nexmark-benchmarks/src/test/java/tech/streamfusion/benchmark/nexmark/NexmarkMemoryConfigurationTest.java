/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class NexmarkMemoryConfigurationTest {
    private static final String KEY = TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS.key();
    private String previous;

    @BeforeEach
    void isolateWeights() {
        previous = System.getProperty(KEY);
        System.clearProperty(KEY);
    }

    @AfterEach
    void restoreWeights() {
        if (previous == null) System.clearProperty(KEY);
        else System.setProperty(KEY, previous);
    }

    @Test
    void leavesFlinkDefaultsAndTaskManagerPrecedenceAvailable() {
        var config = new Configuration();
        NexmarkRowDataJob.configureMemory(config);
        assertThat(config.contains(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS))
                .isFalse();
        assertThat(config.get(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS))
                .isEqualTo(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS.defaultValue());
    }

    @Test
    void preservesWeightsAlreadyProvidedByFlinkConfiguration() {
        var config = new Configuration();
        var weights = Map.of("OPERATOR", "11", "STATE_BACKEND", "7", "PYTHON", "3");
        config.set(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS, weights);
        NexmarkRowDataJob.configureMemory(config);
        assertThat(config.get(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS))
                .isEqualTo(weights);
    }

    @Test
    void explicitBenchmarkMeasurementOverrideUsesTheExistingFlinkSetting() {
        var config = new Configuration();
        config.set(
                TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                Map.of("OPERATOR", "1", "STATE_BACKEND", "1", "PYTHON", "1"));
        System.setProperty(KEY, "OPERATOR:30,STATE_BACKEND:70,PYTHON:30");
        NexmarkRowDataJob.configureMemory(config);
        assertThat(config.get(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS))
                .isEqualTo(Map.of("OPERATOR", "30", "STATE_BACKEND", "70", "PYTHON", "30"));
    }
}
