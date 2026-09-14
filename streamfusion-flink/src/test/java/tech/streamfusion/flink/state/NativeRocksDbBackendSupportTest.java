/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;

@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class NativeRocksDbBackendSupportTest {
    @org.junit.jupiter.api.io.TempDir
    Path temporary;

    @ParameterizedTest
    @CsvSource({"0,memory.fixed-per-slot", "1,memory.managed", "2,options-factory", "3,timer-service.factory"})
    void programmaticOverridesFailBeforeNativeResourcesButPreserveTheRealFlinkFallback(int kind, String option)
            throws Exception {
        for (int phase = 0; phase < 3; phase++) {
            CountingFactory.calls.set(0);
            var directory = temporary.resolve(kind + "-" + phase);
            var delegate = new EmbeddedRocksDBStateBackend();
            delegate.setDbStoragePath(directory.toString());
            if (phase == 0) override(delegate, kind);
            var wrapper = new StreamFusionStateBackend(delegate);
            if (phase != 0) override(delegate, kind); // Settings can change after wrapper construction.
            if (phase == 2) wrapper = org.apache.flink.util.InstantiationUtil.clone(wrapper);
            exercise(wrapper, directory, option);
            if (kind == 2) assertThat(CountingFactory.calls.get()).isPositive();
        }
    }

    @ParameterizedTest
    @CsvSource({
        "state.backend.rocksdb.use-ingest-db-restore-mode,true,use-ingest-db-restore-mode",
        "state.backend.rocksdb.manual-compaction.min-interval,30 s,manual-compaction.min-interval",
        "state.backend.rocksdb.timer-service.factory,HEAP,timer-service.factory",
        "state.backend.rocksdb.metrics.estimate-num-keys,true,metrics",
        "state.backend.latency-track.keyed-state-enabled,true,latency",
        "execution.checkpointing.during-recovery.enabled,true,checkpointing during channel recovery"
    })
    void directWrapperCannotBypassConfiguredAdmission(String key, String value, String reason) throws Exception {
        CountingFactory.calls.set(0);
        var config = new Configuration();
        config.setString(key, value);
        var delegate =
                new EmbeddedRocksDBStateBackend().configure(config, getClass().getClassLoader());
        var directory = temporary.resolve("configured");
        delegate.setDbStoragePath(directory.toString());
        var wrapper = org.apache.flink.util.InstantiationUtil.clone(new StreamFusionStateBackend(delegate, config));
        exercise(wrapper, directory, reason);
    }

    private static void override(EmbeddedRocksDBStateBackend backend, int kind) {
        switch (kind) {
            case 0:
                backend.getMemoryConfiguration().setFixedMemoryPerSlot(MemorySize.ofMebiBytes(16));
                break;
            case 1:
                backend.getMemoryConfiguration().setUseManagedMemory(false);
                break;
            case 2:
                backend.setRocksDBOptions(new CountingFactory());
                break;
            case 3:
                backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
                break;
            default:
                throw new AssertionError(kind);
        }
    }

    @org.junit.jupiter.api.Test
    void runtimeTaskManagerRecoverySettingCannotBeHiddenByTheJobConfiguration() throws Exception {
        var directory = temporary.resolve("runtime-recovery");
        var delegate = new EmbeddedRocksDBStateBackend();
        delegate.setDbStoragePath(directory.toString());
        var provider = NativeCheckpointUploadCancellationTest.localConfig(temporary)
                .getLocalStateDirectoryProvider()
                .orElseThrow();
        var local = org.apache.flink.runtime.state.LocalRecoveryConfig.backupAndRecoveryEnabled(provider);
        exercise(new StreamFusionStateBackend(delegate), directory, "local-to-remote", local);
    }

    private static void exercise(StreamFusionStateBackend wrapper, Path directory, String reason) throws Exception {
        exercise(
                wrapper,
                directory,
                reason,
                org.apache.flink.runtime.state.LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED);
    }

    private static void exercise(
            StreamFusionStateBackend wrapper,
            Path directory,
            String reason,
            org.apache.flink.runtime.state.LocalRecoveryConfig local)
            throws Exception {
        try (var env = new MockEnvironmentBuilder()
                        .setManagedMemorySize(32L << 20)
                        .setTaskStateManager(new org.apache.flink.runtime.state.TestTaskStateManager(local))
                        .build();
                var cancel = new CloseableRegistry()) {
            var stream = new StreamConfig(new Configuration());
            stream.setOperatorID(new OperatorID());
            NativeStateOwnership.register(env, stream, StreamFusionArrowNativeRegionOperator.class);
            var identifier = new OperatorSubtaskDescriptionText(
                            stream.getOperatorID(), StreamFusionArrowNativeRegionOperator.class.getSimpleName(), 0, 1)
                    .toString();
            assertThatThrownBy(() -> wrapper.createKeyedStateBackend(parameters(env, identifier, cancel)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(reason);
            assertThat(env.getMemoryManager().verifyEmpty()).isTrue();
            assertThat(directory).doesNotExist();
            assertThat(CountingFactory.calls.get()).isZero();
            var ordinary = wrapper.createKeyedStateBackend(parameters(env, "ordinary Flink fallback", cancel));
            try {
                // Flink's fixed/unmanaged allocators do not debit managed memory. Native code
                // must not replace either mode with its usual managed STATE_BACKEND lease.
                boolean external = reason.contains("memory.fixed-per-slot") || reason.contains("memory.managed");
                assertThat(env.getMemoryManager().verifyEmpty()).isEqualTo(external);
                var state = ordinary.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("values", IntSerializer.INSTANCE));
                for (int key = 0; key < 257; key++) {
                    ordinary.setCurrentKey(key);
                    state.update(key * 31);
                }
                for (int key = 0; key < 257; key++) {
                    ordinary.setCurrentKey(key);
                    assertThat(state.value()).isEqualTo(key * 31);
                }
            } finally {
                ordinary.close();
                ordinary.dispose();
            }
            assertThat(env.getMemoryManager().verifyEmpty()).isTrue();
        }
    }

    private static KeyedStateBackendParametersImpl<Integer> parameters(
            MockEnvironment env, String identifier, CloseableRegistry cancel) {
        return new KeyedStateBackendParametersImpl<>(
                env,
                env.getJobID(),
                identifier,
                IntSerializer.INSTANCE,
                1,
                new KeyGroupRange(0, 0),
                env.getTaskKvStateRegistry(),
                TtlTimeProvider.DEFAULT,
                env.getMetricGroup(),
                (name, value) -> {},
                List.of(),
                cancel,
                0.5);
    }

    public static final class CountingFactory implements org.apache.flink.state.rocksdb.RocksDBOptionsFactory {
        static final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public org.rocksdb.DBOptions createDBOptions(
                org.rocksdb.DBOptions options, java.util.Collection<AutoCloseable> close) {
            calls.incrementAndGet();
            return options.setMaxOpenFiles(512);
        }

        @Override
        public org.rocksdb.ColumnFamilyOptions createColumnOptions(
                org.rocksdb.ColumnFamilyOptions options, java.util.Collection<AutoCloseable> close) {
            return options;
        }
    }
}
