/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;

class NativeStateOwnershipTest {
    @Test
    void realFlinkIdentifierSelectsOneNativeLeaseWithoutOpeningJavaRocksDb() throws Exception {
        try (var environment = new MockEnvironmentBuilder()
                        .setManagedMemorySize(16L << 20)
                        .build();
                var cancel = new CloseableRegistry()) {
            var config = new StreamConfig(new Configuration());
            config.setOperatorID(new OperatorID());
            config.setOperatorName("any user-visible transformation name");
            NativeStateOwnership.register(environment, config, StreamFusionArrowNativeRegionOperator.class);
            NativeStateOwnership.register(environment, config, StreamFusionArrowNativeRegionOperator.class);
            // Flink discards serialized factories before state initialization. Ownership must
            // survive that phase and must not rely on rereading operator factories.
            new StreamConfig(environment.getTaskConfiguration()).clearInitialConfigs();
            var parameters = parameters(environment, identifier(config, 0, 1), cancel);
            var delegate = new ObservingRocksDbBackend();
            var backend = (StreamFusionKeyedStateBackend<Integer>)
                    new StreamFusionStateBackend(delegate).createKeyedStateBackend(parameters);
            try {
                assertThat(NativeStateOwnership.owns(parameters)).isTrue();
                assertThat(delegate.opened).isFalse();
                assertThat(backend.nativeRocksDbMemoryScope()).isNotNull();
                assertThat(backend.nativeRocksDbMemoryLimit()).isEqualTo(8L << 20);
                assertThat(environment.getMemoryManager().verifyEmpty()).isFalse();
            } finally {
                backend.close();
                backend.dispose();
            }
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    @Test
    void registrationIsExactAndTaskLocalAndDoesNotReplaceOtherKeyedBackends() throws Exception {
        try (var environment = new MockEnvironmentBuilder().build();
                var other = new MockEnvironmentBuilder().build();
                var cancel = new CloseableRegistry()) {
            var first = new StreamConfig(new Configuration());
            first.setOperatorID(new OperatorID());
            var second = new StreamConfig(new Configuration());
            second.setOperatorID(new OperatorID());
            NativeStateOwnership.register(environment, first, StreamFusionArrowNativeRegionOperator.class);
            assertThat(NativeStateOwnership.owns(parameters(environment, identifier(first, 0, 1), cancel)))
                    .isTrue();
            assertThat(NativeStateOwnership.owns(parameters(other, identifier(first, 0, 1), cancel)))
                    .isFalse();
            assertThat(NativeStateOwnership.owns(parameters(environment, identifier(second, 0, 1), cancel)))
                    .isFalse();
            assertThat(NativeStateOwnership.owns(parameters(environment, identifier(first, 1, 2), cancel)))
                    .isFalse();
            NativeStateOwnership.register(environment, second, AbstractStreamFusionArrowKeyedStateOperator.class);
            var legacy = new OperatorSubtaskDescriptionText(
                            second.getOperatorID(),
                            AbstractStreamFusionArrowKeyedStateOperator.class.getSimpleName(),
                            0,
                            1)
                    .toString();
            assertThat(NativeStateOwnership.owns(parameters(environment, legacy, cancel)))
                    .isTrue();
            NativeStateOwnership.register(environment, second, AbstractStreamFusionArrowKeyedStateOperatorV2.class);
            var v2 = new OperatorSubtaskDescriptionText(
                            second.getOperatorID(),
                            AbstractStreamFusionArrowKeyedStateOperatorV2.class.getSimpleName(),
                            0,
                            1)
                    .toString();
            assertThat(NativeStateOwnership.owns(parameters(environment, v2, cancel)))
                    .isTrue();
            assertThatThrownBy(() -> NativeStateOwnership.register(environment, first, String.class))
                    .isInstanceOf(IllegalArgumentException.class);
            var delegate = new ObservingRocksDbBackend();
            var ordinary = parameters(environment, "streamfusion-user-named-operator", cancel);
            assertThat(NativeStateOwnership.owns(ordinary)).isFalse();
            assertThatThrownBy(() -> new StreamFusionStateBackend(delegate).createKeyedStateBackend(ordinary))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Java delegate selected");
            assertThat(delegate.opened).isTrue();
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void formerEmptyJavaRocksDbShellHasNoManagedStateToMigrate(boolean incremental) throws Exception {
        try (var environment = new MockEnvironmentBuilder()
                        .setManagedMemorySize(32L << 20)
                        .build();
                var cancel = new CloseableRegistry()) {
            var config = new StreamConfig(new Configuration());
            config.setOperatorID(new OperatorID());
            var parameters = parameters(environment, identifier(config, 0, 1), cancel);
            var old = new StreamFusionStateBackend(new EmbeddedRocksDBStateBackend(incremental))
                    .createKeyedStateBackend(parameters);
            try {
                var snapshot = old.snapshot(
                        1,
                        1,
                        new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20),
                        org.apache.flink.runtime.checkpoint.CheckpointOptions.forCheckpointWithDefaultLocation());
                snapshot.run();
                assertThat(snapshot.get().getJobManagerOwnedSnapshot()).isNull();
                assertThat(snapshot.get().getTaskLocalSnapshot()).isNull();
            } finally {
                old.close();
                old.dispose();
            }
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
            NativeStateOwnership.register(environment, config, StreamFusionArrowNativeRegionOperator.class);
            var current = (StreamFusionKeyedStateBackend<Integer>)
                    new StreamFusionStateBackend(new ObservingRocksDbBackend()).createKeyedStateBackend(parameters);
            try {
                assertThat(current.nativeRocksDbMemoryLimit()).isEqualTo(16L << 20);
            } finally {
                current.close();
                current.dispose();
            }
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    private static String identifier(StreamConfig config, int index, int parallelism) {
        return new OperatorSubtaskDescriptionText(
                        config.getOperatorID(),
                        StreamFusionArrowNativeRegionOperator.class.getSimpleName(),
                        index,
                        parallelism)
                .toString();
    }

    private static KeyedStateBackendParametersImpl<Integer> parameters(
            MockEnvironment environment, String identifier, CloseableRegistry cancel) {
        return new KeyedStateBackendParametersImpl<>(
                environment,
                environment.getJobID(),
                identifier,
                IntSerializer.INSTANCE,
                1,
                new KeyGroupRange(0, 0),
                environment.getTaskKvStateRegistry(),
                TtlTimeProvider.DEFAULT,
                environment.getMetricGroup(),
                (name, value) -> {},
                List.of(),
                cancel,
                0.5);
    }

    public static final class ObservingRocksDbBackend extends EmbeddedRocksDBStateBackend {
        private boolean opened;

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
                throws IOException {
            opened = true;
            throw new IOException("Java delegate selected");
        }
    }
}
