/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;

class NativeRegionRestoreBootstrapTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void backendOwnsPreparationUntilOperatorClaimsItsContext(boolean claim) throws Exception {
        try (var env = new MockEnvironmentBuilder().build();
                var cancel = new CloseableRegistry();
                var backend = backend()) {
            var config = config();
            var closed = new AtomicInteger();
            AutoCloseable context = closed::incrementAndGet;
            try (var registration = NativeRegionRestoreBootstrap.register(
                    env, config, StreamFusionArrowNativeRegionOperator.class, Set.of(), (owner, initialization) -> {
                        assertThat(owner).isSameAs(backend);
                        return context;
                    })) {
                NativeRegionRestoreBootstrap.prepare(backend, parameters(env, config, cancel));
                if (claim) backend.claimPreparedNativeRegion();
                backend.close();
                assertThat(closed.get()).isEqualTo(claim ? 0 : 1);
                if (claim) context.close();
                else assertThatThrownBy(backend::claimPreparedNativeRegion).hasMessageContaining("closed before");
            }
            assertThat(closed.get()).isOne();
        }
    }

    @Test
    void registrationIsTaskScopedAndAClosedOwnerCannotRemoveItsReplacement() throws Exception {
        try (var first = new MockEnvironmentBuilder().build();
                var second = new MockEnvironmentBuilder().build();
                var cancel = new CloseableRegistry();
                var backend = backend()) {
            var config = config();
            var calls = new AtomicInteger();
            NativeRegionRestoreBootstrap.Prepare factory = (owner, initialization) -> {
                calls.incrementAndGet();
                return () -> {};
            };
            var old = NativeRegionRestoreBootstrap.register(
                    first, config, StreamFusionArrowNativeRegionOperator.class, Set.of(), factory);
            try {
                assertThatThrownBy(() -> NativeRegionRestoreBootstrap.register(
                                first, config, StreamFusionArrowNativeRegionOperator.class, Set.of(), factory))
                        .hasMessageContaining("already registered");
                NativeRegionRestoreBootstrap.prepare(backend, parameters(second, config, cancel));
                assertThat(calls.get()).isZero();
            } finally {
                old.close();
            }
            try (var replacement = NativeRegionRestoreBootstrap.register(
                    first, config, StreamFusionArrowNativeRegionOperator.class, Set.of(), factory)) {
                old.close();
                NativeRegionRestoreBootstrap.prepare(backend, parameters(first, config, cancel));
                assertThat(calls.get()).isOne();
            }
        }
    }

    private static StreamConfig config() {
        var result = new StreamConfig(new Configuration());
        result.setOperatorID(new OperatorID());
        return result;
    }

    private static StateBackend.KeyedStateBackendParameters<?> parameters(
            MockEnvironment env, StreamConfig config, CloseableRegistry cancellation) {
        return (StateBackend.KeyedStateBackendParameters<?>) Proxy.newProxyInstance(
                NativeRegionRestoreBootstrapTest.class.getClassLoader(),
                new Class<?>[] {StateBackend.KeyedStateBackendParameters.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getEnv":
                            return env;
                        case "getCancelStreamRegistry":
                            return cancellation;
                        case "getOperatorIdentifier":
                            return NativeStateOwnership.identifier(
                                    env, config, StreamFusionArrowNativeRegionOperator.class);
                        default:
                            throw new UnsupportedOperationException(method.toString());
                    }
                });
    }

    private static StreamFusionKeyedStateBackend<?> backend() {
        var delegate = (CheckpointableKeyedStateBackend<?>) Proxy.newProxyInstance(
                NativeRegionRestoreBootstrapTest.class.getClassLoader(),
                new Class<?>[] {CheckpointableKeyedStateBackend.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getKeyGroupRange")) return new KeyGroupRange(0, 0);
                    if (method.getName().equals("close") || method.getName().equals("dispose")) return null;
                    throw new UnsupportedOperationException(method.toString());
                });
        return new StreamFusionKeyedStateBackend<>(delegate, List.of(), "rocksdb", null, true);
    }
}
