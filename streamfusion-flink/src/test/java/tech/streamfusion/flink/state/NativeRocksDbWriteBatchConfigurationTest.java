/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.proto.plan.v1.NativeRocksDbOptions;

class NativeRocksDbWriteBatchConfigurationTest {
    @org.junit.jupiter.api.io.TempDir
    Path temporary;

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 50, 4096, 2097152, Long.MAX_VALUE})
    void resolvedAndProgrammaticThresholdsSurviveBackendSerialization(long bytes) throws Exception {
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBConfigurableOptions.WRITE_BATCH_SIZE, new MemorySize(bytes));
        var flink =
                new EmbeddedRocksDBStateBackend().configure(config, getClass().getClassLoader());
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
        assertThat(NativeRocksDbConfiguration.fromConfig(config).getWriteBatchSize())
                .isEqualTo(flink.getWriteBatchSize());
        // The backend's public setter takes precedence over the original configuration.
        long override = bytes == 0 ? 123 : 0;
        flink.setWriteBatchSize(override);
        var wrapper = org.apache.flink.util.InstantiationUtil.clone(new StreamFusionStateBackend(flink, config));
        var field = StreamFusionStateBackend.class.getDeclaredField("nativeRocksDbOptions");
        field.setAccessible(true);
        assertThat(NativeRocksDbOptions.parseFrom((byte[]) field.get(wrapper)).getWriteBatchSize())
                .isEqualTo(override);
    }
}
