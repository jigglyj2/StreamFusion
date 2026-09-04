/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
        install((Configuration) environment.getConfiguration());
    }

    /** Installs the delegating backend in the configuration used to create the real pipeline. */
    public static void install(Configuration configuration) {
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
            StateBackend delegate = loadDelegate(delegateConfig, classLoader);
            return new StreamFusionStateBackend(delegate);
        } catch (ReflectiveOperationException error) {
            throw new IllegalConfigurationException(
                    "Could not load delegated Flink state backend " + delegateName, error);
        }
    }

    private static StateBackend loadDelegate(Configuration configuration, ClassLoader classLoader)
            throws ReflectiveOperationException, IOException {
        // The third StateBackendLoader argument is SLF4J Logger. Calling it directly lets the
        // shaded runtime rewrite that external signature, which then fails against Flink's
        // unshaded API. Resolve the method by its stable first two parameters instead.
        for (Method method : StateBackendLoader.class.getMethods()) {
            Class<?>[] parameters = method.getParameterTypes();
            if (method.getName().equals("loadStateBackendFromConfig")
                    && parameters.length == 3
                    && ReadableConfig.class.isAssignableFrom(parameters[0])
                    && parameters[1] == ClassLoader.class) {
                try {
                    return (StateBackend) method.invoke(null, configuration, classLoader, null);
                } catch (InvocationTargetException error) {
                    Throwable cause = error.getCause();
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    if (cause instanceof ReflectiveOperationException) {
                        throw (ReflectiveOperationException) cause;
                    }
                    throw new IllegalConfigurationException("Could not load delegated Flink state backend", cause);
                }
            }
        }
        throw new NoSuchMethodException("Flink StateBackendLoader.loadStateBackendFromConfig");
    }
}
