/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.IOException;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Internal factory installed by the planner while retaining the user's Flink backend choice. */
public final class StreamFusionStateBackendFactory implements StateBackendFactory<StreamFusionStateBackend> {
    private static final String FACTORY_NAME = StreamFusionStateBackendFactory.class.getName();
    private static final ConfigOption<String> DELEGATE_BACKEND = ConfigOptions.key(
                    "streamfusion.internal.delegate-state-backend")
            .stringType()
            .defaultValue(StateBackendLoader.HASHMAP_STATE_BACKEND_NAME);

    public static void install(StreamExecutionEnvironment environment) {
        if (!(environment.getConfiguration() instanceof Configuration)) {
            throw new IllegalStateException("Flink execution configuration is not mutable");
        }
        Configuration configuration = (Configuration) environment.getConfiguration();
        String configured = configuration.get(StateBackendOptions.STATE_BACKEND);
        if (FACTORY_NAME.equals(configured)) {
            return;
        }
        configuration.set(DELEGATE_BACKEND, configured);
        configuration.set(StateBackendOptions.STATE_BACKEND, FACTORY_NAME);
    }

    @Override
    public StreamFusionStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException, IOException {
        if (!(config instanceof Configuration)) {
            throw new IllegalConfigurationException("StreamFusion requires Flink Configuration state-backend input");
        }
        Configuration delegateConfig = new Configuration((Configuration) config);
        String delegateName = config.get(DELEGATE_BACKEND);
        if (FACTORY_NAME.equals(delegateName)) {
            throw new IllegalConfigurationException("StreamFusion state backend cannot delegate to itself");
        }
        delegateConfig.set(StateBackendOptions.STATE_BACKEND, delegateName);
        try {
            StateBackend delegate = StateBackendLoader.loadStateBackendFromConfig(delegateConfig, classLoader, null);
            return new StreamFusionStateBackend(delegate);
        } catch (org.apache.flink.util.DynamicCodeLoadingException error) {
            throw new IllegalConfigurationException(
                    "Could not load delegated Flink state backend " + delegateName, error);
        }
    }
}
