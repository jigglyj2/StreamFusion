/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.junit.jupiter.api.Test;

class NativeStateConfigurationSupportTest {
    @Test
    void flinkMemoryBudgetsAndIncrementalCheckpointsRemainConfigurable() {
        for (String backend : new String[] {"hashmap", "rocksdb"}) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, backend);
            config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(512));
            config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
        }
    }

    @Test
    void rejectsNativeOptionsInsteadOfSilentlyUsingDefaults() {
        for (var option : new String[][] {
            {"state.backend.rocksdb.memory.managed", "false"},
            {"state.backend.rocksdb.memory.fixed-per-slot", "128 mb"},
            {"state.backend.rocksdb.options-factory", "example.CustomFactory"},
            {"state.backend.rocksdb.log.level", "NUM_INFO_LOG_LEVELS"},
            {"state.backend.rocksdb.compression.per.level", "XPRESS_COMPRESSION"}
        }) {
            var config = new Configuration();
            config.setString("state.backend", "rocksdb"); // Flink's deprecated backend alias.
            config.setString(option[0], option[1]);
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .as(option[0])
                    .contains(option[0]);
            config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
        }
    }

    @Test
    void productionAdmissionRetainsBackendIdentityAfterInstallingTheWrapper() {
        for (String backend : new String[] {"hashmap", "rocksdb"}) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, backend);
            String expected = NativeStateSupport.unsupportedReason(config);
            assertThat(expected).isNull();
            StreamFusionStateBackendFactory.install(config);
            StreamFusionStateBackendFactory.install(config);
            assertThat(NativeStateSupport.unsupportedReason(config)).isEqualTo(expected);
            config.set(RocksDBOptions.USE_MANAGED_MEMORY, false);
            if (backend.equals("rocksdb"))
                assertThat(NativeStateSupport.unsupportedReason(config))
                        .contains(RocksDBOptions.USE_MANAGED_MEMORY.key());
        }
    }

    @Test
    void checkpointingDuringRecoveryExplainsTheUpstreamRecaptureBoundary() {
        var config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED, true);
        assertThat(NativeStateSupport.unsupportedReason(config))
                .contains(
                        "checkpointing during channel recovery",
                        CheckpointingOptions.CHECKPOINTING_DURING_RECOVERY_ENABLED.key(),
                        "Flink 2.3 local channels can recapture unread recovered buffers");
    }

    @Test
    void explicitTypedDefaultsAndIrrelevantOptionsDoNotCauseFallback() {
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBOptions.USE_MANAGED_MEMORY, true);
        config.set(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, MemorySize.ofMebiBytes(64));
        config.setString("unrelated.application.option", "value");
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
        config.set(StateBackendOptions.STATE_BACKEND, "forst");
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).contains("forst");
    }
}
