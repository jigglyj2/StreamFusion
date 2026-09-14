/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BackendRestorerProcedure;

/** Task-local startup callbacks, never serialized plans or data-plane dispatch. */
public final class NativeRegionRestoreBootstrap {
    private static final Map<Environment, Map<String, WeakReference<Registration>>> REGISTRATIONS = new WeakHashMap<>();

    private NativeRegionRestoreBootstrap() {}

    @FunctionalInterface
    public interface Prepare {
        AutoCloseable initialize(StreamFusionKeyedStateBackend<?> backend, StateInitializationContext context)
                throws Exception;
    }

    public static Registration register(
            Environment environment,
            StreamConfig config,
            Class<?> operatorClass,
            Set<String> clockStateNames,
            Prepare prepare) {
        var registration = new Registration(environment, config, operatorClass, clockStateNames, prepare);
        synchronized (REGISTRATIONS) {
            var task = REGISTRATIONS.computeIfAbsent(environment, ignored -> new HashMap<>());
            var previous = task.get(registration.identifier);
            if (previous != null && previous.get() != null)
                throw new IllegalStateException("Native restore owner already registered");
            // Both sides are weak: a callback can capture its operator and environment without
            // making either a process-lifetime root after failed task construction.
            task.put(registration.identifier, new WeakReference<>(registration));
        }
        return registration;
    }

    static boolean isRegistered(StateBackend.KeyedStateBackendParameters<?> parameters) {
        synchronized (REGISTRATIONS) {
            var task = REGISTRATIONS.get(parameters.getEnv());
            var reference = task == null ? null : task.get(parameters.getOperatorIdentifier());
            return reference != null && reference.get() != null;
        }
    }

    static void prepare(
            StreamFusionKeyedStateBackend<?> backend, StateBackend.KeyedStateBackendParameters<?> parameters)
            throws Exception {
        Registration registration;
        synchronized (REGISTRATIONS) {
            var task = REGISTRATIONS.get(parameters.getEnv());
            var reference = task == null ? null : task.get(parameters.getOperatorIdentifier());
            registration = reference == null ? null : reference.get();
        }
        if (registration != null) registration.prepare(backend, parameters.getCancelStreamRegistry());
    }

    public static final class Registration implements AutoCloseable {
        private final Environment environment;
        private final org.apache.flink.runtime.jobgraph.OperatorID operatorId;
        private final String identifier;
        private final Set<String> clockStateNames;
        private final Prepare factory;
        private boolean closed;

        private Registration(
                Environment environment,
                StreamConfig config,
                Class<?> operatorClass,
                Set<String> clockStateNames,
                Prepare factory) {
            this.environment = environment;
            this.operatorId = config.getOperatorID();
            this.identifier = NativeStateOwnership.identifier(environment, config, operatorClass);
            this.clockStateNames = Set.copyOf(clockStateNames);
            this.factory = factory;
        }

        private synchronized void prepare(StreamFusionKeyedStateBackend<?> backend, CloseableRegistry cancellation)
                throws Exception {
            if (closed) throw new IllegalStateException("Native restore registration closed");
            var prioritized = environment.getTaskStateManager().prioritizedOperatorState(operatorId);
            if (clockStateNames.isEmpty()) {
                backend.ownPreparedNativeRegion(factory.initialize(
                        backend,
                        new StateInitializationContextImpl(
                                prioritized.getRestoredCheckpointId(), null, null, List.of(), List.of())));
                return;
            }
            // Flink initializes its keyed backend before its operator backend. Read only the
            // union-clock partitions with Flink's own deserializer; normal operator restoration
            // still owns the original handles and later verifies the same clock values.
            try (var lifetime = new CloseableRegistry()) {
                var restorer = new BackendRestorerProcedure<DefaultOperatorStateBackend, OperatorStateHandle>(
                        handles -> {
                            var clocks = new java.util.ArrayList<OperatorStateHandle>();
                            for (var handle : handles) {
                                var offsets = new HashMap<String, OperatorStateHandle.StateMetaInfo>();
                                handle.getStateNameToPartitionOffsets().forEach((name, value) -> {
                                    if (clockStateNames.contains(name)) offsets.put(name, value);
                                });
                                if (!offsets.isEmpty())
                                    clocks.add(new OperatorStreamStateHandle(offsets, handle.getDelegateStateHandle()));
                            }
                            return new DefaultOperatorStateBackendBuilder(
                                            environment.getUserCodeClassLoader().asClassLoader(),
                                            environment.getExecutionConfig(),
                                            false,
                                            clocks,
                                            cancellation)
                                    .build();
                        },
                        lifetime,
                        "native region union-clock preparation");
                var clocks = restorer.createAndRestore(
                        prioritized.getPrioritizedManagedOperatorState(),
                        StateObject.StateObjectSizeStatsCollector.create());
                try {
                    backend.ownPreparedNativeRegion(factory.initialize(
                            backend,
                            new StateInitializationContextImpl(
                                    prioritized.getRestoredCheckpointId(), clocks, null, List.of(), List.of())));
                } finally {
                    clocks.dispose();
                }
            }
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            synchronized (REGISTRATIONS) {
                var task = REGISTRATIONS.get(environment);
                if (task != null) {
                    var reference = task.get(identifier);
                    if (reference != null && reference.get() == this) task.remove(identifier);
                    if (task.isEmpty()) REGISTRATIONS.remove(environment);
                }
            }
        }
    }
}
