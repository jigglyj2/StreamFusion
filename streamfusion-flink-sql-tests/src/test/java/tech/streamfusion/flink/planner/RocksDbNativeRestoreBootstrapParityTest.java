/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.state.StreamFusionKeyedStateBackend;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class RocksDbNativeRestoreBootstrapParityTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void nativeOpenAndImportCompleteBeforeBackendReturnsAndCorruptionFailsInThatBoundary(int checkpointMode)
            throws Exception {
        var live = new ArrayList<GenericRowData>();
        try (var oracle = SharedAggregateFlinkOracle.create(true, 0, true);
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState snapshot;
            try (var source = SharedAggregateRuntimeHarness.configuredBackend(new ProbeBackend(), null)) {
                for (int seed = 0; seed < 3; seed++)
                    SharedAggregateCheckpointTest.compare(source, oracle, allocator, live, seed);
                snapshot = SharedAggregateCheckpointTest.snapshot(source, checkpointMode, 7);
            }
            try {
                var restored = new ProbeBackend(true);
                try (var target = SharedAggregateRuntimeHarness.configuredBackend(restored, snapshot)) {
                    assertThat(restored.completedRestores).isOne();
                    assertThat(restored.failedRestores).isOne();
                    // The failed candidate must remove its metric registration so the successful
                    // region publishes a complete fresh Flink surface under the same operator ID.
                    var flinkMetrics = oracle.getOperator().getMetricGroup();
                    flinkMetrics.gauge(
                            "currentInputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                    flinkMetrics.gauge(
                            "currentOutputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                    SharedAggregateMetricSurfaceTest.compare(
                            SharedAggregateMetricSurfaceTest.metrics(flinkMetrics),
                            SharedAggregateMetricSurfaceTest.metrics(
                                    SharedAggregateMetricSurfaceTest.stageGroup(target, 3)));
                    for (int seed = 3; seed < 6; seed++)
                        SharedAggregateCheckpointTest.compare(target, oracle, allocator, live, seed);
                }
            } finally {
                snapshot.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static IncrementalRemoteKeyedStateHandle corruptCurrent(IncrementalRemoteKeyedStateHandle original) {
        var files = new ArrayList<HandleAndLocalPath>();
        boolean replaced = false;
        for (var file : original.getPrivateState()) {
            if (file.getLocalPath().endsWith("/CURRENT")) {
                files.add(HandleAndLocalPath.of(
                        new ByteStreamStateHandle(
                                "corrupt-current",
                                "not-a-valid-manifest\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                        file.getLocalPath()));
                replaced = true;
            } else files.add(file);
        }
        assertThat(replaced).isTrue();
        var corrupt = new IncrementalRemoteKeyedStateHandle(
                original.getBackendIdentifier(),
                original.getKeyGroupRange(),
                original.getCheckpointId(),
                original.getSharedState(),
                files,
                original.getMetaDataStateHandle(),
                original.getCheckpointedSize());
        // Borrow all original remote handles: only the original snapshot owns their discard.
        return corrupt;
    }

    private static final class ProbeBackend implements org.apache.flink.runtime.state.StateBackend {
        final StreamFusionStateBackend delegate = new StreamFusionStateBackend(new EmbeddedRocksDBStateBackend(true));
        final boolean retry;
        int completedRestores;
        int failedRestores;

        ProbeBackend() {
            this(false);
        }

        ProbeBackend(boolean retry) {
            this.retry = retry;
        }

        @Override
        public boolean useManagedMemory() {
            return delegate.useManagedMemory();
        }

        @Override
        public org.apache.flink.runtime.state.OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            return delegate.createOperatorStateBackend(parameters);
        }

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
                throws Exception {
            if (retry && !parameters.getStateHandles().isEmpty()) {
                assertThat(parameters.getStateHandles()).hasSize(1);
                var original = (IncrementalRemoteKeyedStateHandle)
                        parameters.getStateHandles().iterator().next();
                try (var lifetime = new org.apache.flink.core.fs.CloseableRegistry()) {
                    var restorer = new org.apache.flink.streaming.api.operators.BackendRestorerProcedure<
                            CheckpointableKeyedStateBackend<K>, org.apache.flink.runtime.state.KeyedStateHandle>(
                            handles -> {
                                var candidate = new org.apache.flink.runtime.state.KeyedStateBackendParametersImpl<>(
                                        parameters);
                                candidate.setStateHandles(handles);
                                return createCandidate(candidate);
                            },
                            lifetime,
                            "native database candidate validation");
                    var result = restorer.createAndRestore(
                            List.of(List.of(corruptCurrent(original)), List.of(original)),
                            org.apache.flink.runtime.state.StateObject.StateObjectSizeStatsCollector.create());
                    lifetime.unregisterCloseable(result); // The outer Flink initializer now owns this backend.
                    return result;
                }
            }
            return createCandidate(parameters);
        }

        private <K> CheckpointableKeyedStateBackend<K> createCandidate(KeyedStateBackendParameters<K> parameters)
                throws Exception {
            boolean physical =
                    parameters.getStateHandles().stream().anyMatch(IncrementalRemoteKeyedStateHandle.class::isInstance);
            try {
                var backend = delegate.createKeyedStateBackend(parameters);
                if (physical) {
                    assertThat(((StreamFusionKeyedStateBackend<?>) backend).usesNativeFileCheckpoints())
                            .isTrue();
                    completedRestores++;
                }
                return backend;
            } catch (Exception | Error failure) {
                if (physical) {
                    failedRestores++;
                    assertThat(parameters.getEnv().getMemoryManager().verifyEmpty())
                            .isTrue();
                }
                throw failure;
            }
        }
    }
}
