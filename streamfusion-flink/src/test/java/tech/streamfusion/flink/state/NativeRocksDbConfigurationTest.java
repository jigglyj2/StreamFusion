/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.RocksDB;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;
import tech.streamfusion.proto.plan.v1.NativeStateBindings;

class NativeRocksDbConfigurationTest {
    @Test
    void generatedSettingsMatchActualFlinkResourceContainerAndWireContract() throws Exception {
        RocksDB.loadLibrary();
        for (int seed = 0; seed < 8; seed++) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            config.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 2 + seed);
            config.set(RocksDBConfigurableOptions.MAX_OPEN_FILES, 64 + seed);
            config.set(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE, new MemorySize(seed % 2 == 0 ? 0 : (8L << 20)));
            config.set(RocksDBConfigurableOptions.LOG_FILE_NUM, 2 + seed);
            config.set(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, seed % 2 == 0);
            config.set(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, new MemorySize((8L + seed) << 20));
            config.set(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE, new MemorySize((32L + seed) << 20));
            config.set(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, new MemorySize((4L + seed) << 20));
            config.set(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, 3 + seed);
            config.set(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 2);
            config.set(
                    RocksDBConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME,
                    Duration.ofSeconds(seed % 2 == 0 ? 0 : 7200));
            config.set(RocksDBConfigurableOptions.BLOCK_SIZE, new MemorySize(4096L * (seed + 1)));
            config.set(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE, new MemorySize(1024L * (seed + 1)));

            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
            var resolved = NativeRocksDbConfiguration.fromConfig(config);
            try (var flink = new RocksDBResourceContainer(config, PredefinedOptions.DEFAULT, null, null, null, false)) {
                var db = flink.getDbOptions();
                var cf = flink.getColumnOptions();
                var table = (BlockBasedTableConfig) cf.tableFormatConfig();
                assertThat(resolved.getMaxBackgroundJobs()).isEqualTo(db.maxBackgroundJobs());
                assertThat(resolved.getMaxOpenFiles()).isEqualTo(db.maxOpenFiles());
                assertThat(resolved.getMaxLogFileSize()).isEqualTo(db.maxLogFileSize());
                assertThat(resolved.getKeepLogFileNum()).isEqualTo(db.keepLogFileNum());
                assertThat(resolved.getDynamicLevelBytes()).isEqualTo(cf.levelCompactionDynamicLevelBytes());
                assertThat(resolved.getTargetFileSizeBase()).isEqualTo(cf.targetFileSizeBase());
                assertThat(resolved.getMaxBytesForLevelBase()).isEqualTo(cf.maxBytesForLevelBase());
                assertThat(resolved.getWriteBufferSize()).isEqualTo(cf.writeBufferSize());
                assertThat(resolved.getMaxWriteBufferNumber()).isEqualTo(cf.maxWriteBufferNumber());
                assertThat(resolved.getMinWriteBufferNumberToMerge()).isEqualTo(cf.minWriteBufferNumberToMerge());
                assertThat(resolved.getPeriodicCompactionSeconds()).isEqualTo(cf.periodicCompactionSeconds());
                assertThat(resolved.getBlockSize()).isEqualTo(table.blockSize());
                assertThat(resolved.getMetadataBlockSize()).isEqualTo(table.metadataBlockSize());
            }
            var binding = NativeStateBinding.newBuilder()
                    .setPlanNodeId(3)
                    .setRocksdb(tech.streamfusion.proto.plan.v1.NativeRocksDbState.getDefaultInstance())
                    .build();
            binding = NativeRocksDbConfiguration.bind(binding, resolved);
            var wire = NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(binding)));
            assertThat(wire.getProtocolVersion()).isEqualTo(10);
            assertThat(wire.getBindings(0).getRocksdb().getDatabaseOptions()).isEqualTo(resolved);
        }
    }

    @Test
    void invalidSettingsFallBackWithTheExactFlinkOptionName() throws Exception {
        for (var option : List.of(
                RocksDBConfigurableOptions.BLOCK_SIZE,
                RocksDBConfigurableOptions.WRITE_BUFFER_SIZE,
                RocksDBConfigurableOptions.METADATA_BLOCK_SIZE)) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            config.set(option, MemorySize.ZERO);
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .contains(option.key());
            assertThatThrownBy(() -> NativeRocksDbConfiguration.fromConfig(config))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 0);
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                .contains(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS.key());
        config.removeConfig(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS);
        config.set(RocksDBConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME, Duration.ofSeconds(-1));
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                .contains(RocksDBConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME.key());
    }

    @Test
    void supportedPresetsAndExplicitDefaultsMatchFlinksPrecedence() throws Exception {
        RocksDB.loadLibrary();
        for (var preset : List.of(
                PredefinedOptions.DEFAULT,
                PredefinedOptions.FLASH_SSD_OPTIMIZED,
                PredefinedOptions.SPINNING_DISK_OPTIMIZED,
                PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)) {
            for (boolean override : new boolean[] {false, true}) {
                var config = new Configuration();
                config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
                config.set(org.apache.flink.state.rocksdb.RocksDBOptions.PREDEFINED_OPTIONS, preset.name());
                if (override) {
                    config.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 2);
                    config.set(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, false);
                    config.set(RocksDBConfigurableOptions.MAX_OPEN_FILES, 128);
                }
                assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                        .isNull();
                var resolved = NativeRocksDbConfiguration.fromConfig(config);
                try (var flink = new RocksDBResourceContainer(config, preset, null, null, null, false)) {
                    assertThat(resolved.getMaxBackgroundJobs())
                            .isEqualTo(flink.getDbOptions().maxBackgroundJobs());
                    assertThat(resolved.getMaxOpenFiles())
                            .isEqualTo(flink.getDbOptions().maxOpenFiles());
                    assertThat(resolved.getDynamicLevelBytes())
                            .isEqualTo(flink.getColumnOptions().levelCompactionDynamicLevelBytes());
                }
            }
        }
    }

    @Test
    void programmaticPresetSurvivesBackendSerialization() throws Exception {
        var delegate = new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true);
        delegate.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
        var backend = new StreamFusionStateBackend(delegate);
        backend = org.apache.flink.util.InstantiationUtil.clone(backend);
        var field = StreamFusionStateBackend.class.getDeclaredField("nativeRocksDbOptions");
        field.setAccessible(true);
        var resolved = tech.streamfusion.proto.plan.v1.NativeRocksDbOptions.parseFrom((byte[]) field.get(backend));
        assertThat(resolved.getMaxBackgroundJobs()).isEqualTo(4);
        assertThat(resolved.getDynamicLevelBytes()).isTrue();
    }
}
