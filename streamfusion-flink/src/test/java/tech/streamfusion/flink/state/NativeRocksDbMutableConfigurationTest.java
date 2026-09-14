/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeRegionOperator;
import tech.streamfusion.proto.plan.v1.NativeRocksDbOptions;
import tech.streamfusion.proto.plan.v1.NativeRocksDbState;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;

class NativeRocksDbMutableConfigurationTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @EnumSource(PredefinedOptions.class)
    void keyedBackendCreationReadsCurrentPublicSettingsAndPreservesExplicitOverrides(PredefinedOptions preset)
            throws Exception {
        org.rocksdb.RocksDB.loadLibrary();
        for (boolean explicit : List.of(false, true)) {
            var config = new Configuration();
            if (explicit) {
                config.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 2);
                config.set(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, false);
                config.set(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, MemorySize.ofMebiBytes(80));
            }
            var delegate = new EmbeddedRocksDBStateBackend(true)
                    .configure(config, getClass().getClassLoader());
            delegate.setDbStoragePath(temporary.resolve(preset + "-" + explicit).toString());
            // The delegate keeps its own configuration copy. The wrapper must do so too.
            var wrapper = new StreamFusionStateBackend(delegate, config);
            config.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 99);
            delegate.setPredefinedOptions(preset);
            delegate.setWriteBatchSize(50);
            for (int phase = 0; phase < 2; phase++) {
                var actual = createOptions(phase == 0 ? wrapper : InstantiationUtil.clone(wrapper));
                var factory = EmbeddedRocksDBStateBackend.class.getDeclaredMethod(
                        "createOptionsAndResourceContainer", java.io.File.class);
                factory.setAccessible(true);
                try (var flink = (RocksDBResourceContainer) factory.invoke(delegate, new Object[] {null})) {
                    var db = flink.getDbOptions();
                    var cf = flink.getColumnOptions();
                    assertThat(actual.getMaxBackgroundJobs()).isEqualTo(db.maxBackgroundJobs());
                    assertThat(actual.getDynamicLevelBytes()).isEqualTo(cf.levelCompactionDynamicLevelBytes());
                    assertThat(actual.getTargetFileSizeBase()).isEqualTo(cf.targetFileSizeBase());
                    assertThat(actual.getMaxWriteBufferNumber()).isEqualTo(cf.maxWriteBufferNumber());
                    assertThat(actual.getMinWriteBufferNumberToMerge()).isEqualTo(cf.minWriteBufferNumberToMerge());
                    assertThat(actual.getWriteBatchSize()).isEqualTo(delegate.getWriteBatchSize());
                }
            }
            // A second task creation can see a later setter value without changing an open DB.
            delegate.setWriteBatchSize(0);
            delegate.setPredefinedOptions(PredefinedOptions.DEFAULT);
            assertThat(createOptions(wrapper).getWriteBatchSize()).isZero();
            assertThat(createOptions(wrapper).getMaxWriteBufferNumber()).isEqualTo(2);
        }
    }

    @Test
    void oldSerializedConfigurationCannotGuessWhichValuesOverrideThePreset() throws Exception {
        var wrapper = new StreamFusionStateBackend(new EmbeddedRocksDBStateBackend());
        var field = StreamFusionStateBackend.class.getDeclaredField("nativeRocksDbExplicitOptions");
        field.setAccessible(true);
        field.set(wrapper, null); // Java deserialization initializes a missing historical field to null.
        assertThatThrownBy(() -> createOptions(wrapper)).hasMessageContaining("explicit option provenance");
    }

    private NativeRocksDbOptions createOptions(StreamFusionStateBackend wrapper) throws Exception {
        try (var env = new MockEnvironmentBuilder()
                        .setManagedMemorySize(32L << 20)
                        .build();
                var cancel = new CloseableRegistry()) {
            var stream = new StreamConfig(new Configuration());
            stream.setOperatorID(new OperatorID());
            NativeStateOwnership.register(env, stream, StreamFusionArrowNativeRegionOperator.class);
            var identifier = new OperatorSubtaskDescriptionText(
                            stream.getOperatorID(), StreamFusionArrowNativeRegionOperator.class.getSimpleName(), 0, 1)
                    .toString();
            try {
                var backend = (StreamFusionKeyedStateBackend<Integer>)
                        wrapper.createKeyedStateBackend(new KeyedStateBackendParametersImpl<>(
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
                                0.5));
                try {
                    return backend.bindNativeRocksDbConfiguration(NativeStateBinding.newBuilder()
                                    .setPlanNodeId(3)
                                    .setRocksdb(NativeRocksDbState.getDefaultInstance())
                                    .build())
                            .getRocksdb()
                            .getDatabaseOptions();
                } finally {
                    backend.close();
                    backend.dispose();
                }
            } finally {
                assertThat(env.getMemoryManager().verifyEmpty()).isTrue();
            }
        }
    }
}
