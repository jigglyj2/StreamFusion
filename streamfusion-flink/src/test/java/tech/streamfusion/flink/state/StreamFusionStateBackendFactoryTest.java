/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamFusionStateBackendFactoryTest {
    @Test
    void recognizesOnlyPlannerOwnedNativeOperatorIdentifiers() {
        assertThat(StreamFusionStateBackend.isNativeOperator("streamfusion-incremental-group-aggregate_deadbeef_(1/1)"))
                .isTrue();
        assertThat(StreamFusionStateBackend.isNativeOperator("flink-streamfusion-lookalike_deadbeef_(1/1)"))
                .isFalse();
        assertThat(StreamFusionStateBackend.isNativeOperator(null)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void preservesFlinksConfiguredBackendWithoutAStreamFusionToggle(String configuredBackend) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, configuredBackend);
        StreamExecutionEnvironment environment = new StreamExecutionEnvironment(configuration);

        StreamFusionStateBackendFactory.install(environment);

        Configuration installed = (Configuration) environment.getConfiguration();
        assertThat(installed.get(StateBackendOptions.STATE_BACKEND))
                .isEqualTo(StreamFusionStateBackendFactory.class.getName());
        StreamFusionStateBackend backend = new StreamFusionStateBackendFactory()
                .createFromConfig(installed, getClass().getClassLoader());
        assertThat(backend.getName().toLowerCase()).contains(configuredBackend.replace("hashmap", "hash"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void installsIntoTheTableConfigurationUsedForPipelineCreation(String configuredBackend) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, configuredBackend);

        StreamFusionStateBackendFactory.install(configuration);

        assertThat(configuration.get(StateBackendOptions.STATE_BACKEND))
                .isEqualTo(StreamFusionStateBackendFactory.class.getName());
        StreamFusionStateBackend backend = new StreamFusionStateBackendFactory()
                .createFromConfig(configuration, getClass().getClassLoader());
        assertThat(backend.getName().toLowerCase()).contains(configuredBackend.replace("hashmap", "hash"));
    }
}
