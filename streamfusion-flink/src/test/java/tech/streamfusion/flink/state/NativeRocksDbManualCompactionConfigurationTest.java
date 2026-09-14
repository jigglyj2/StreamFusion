/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionConfig;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionManager;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeRocksDbManualCompactionConfigurationTest {
    @ParameterizedTest
    @ValueSource(longs = {0, 1, 999999})
    void dormantSettingsMatchFlinksActualNoOpManagerIncludingSubMillisecondIntervals(long nanos) throws Exception {
        var config = configured(nanos);
        var resolved = RocksDBManualCompactionConfig.from(config);
        assertThat(resolved.minInterval).isZero();
        assertThat(resolved.maxManualCompactions).isEqualTo(2);
        assertThat(resolved.maxFileSizeToCompact).isEqualTo(new MemorySize(100 << 10));
        assertThat(resolved.minFilesToCompact).isEqualTo(3);
        assertThat(resolved.maxFilesToCompact).isEqualTo(20);
        assertThat(resolved.maxOutputFileSize).isEqualTo(MemorySize.ofMebiBytes(32));
        assertThat(resolved.maxAutoCompactions).isEqualTo(2);
        // Flink's disabled branch needs neither a database nor an executor and allocates no scheduler.
        try (var manager = RocksDBManualCompactionManager.create(null, resolved, null)) {
            assertThat(manager).isSameAs(RocksDBManualCompactionManager.NO_OP);
            manager.start();
            manager.register(null);
        }
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
    }

    @ParameterizedTest
    @ValueSource(longs = {1000000, 30000000000L})
    void enabledSchedulerRetainsAnExplicitPrimaryFallbackReason(long nanos) {
        var config = configured(nanos);
        assertThat(RocksDBManualCompactionConfig.from(config).minInterval).isPositive();
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                .contains(RocksDBManualCompactionOptions.MIN_INTERVAL.key(), "small-SST selection", "scheduler");
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "min-interval",
                "max-parallel-compactions",
                "max-file-size-to-compact",
                "min-files-to-compact",
                "max-files-to-compact",
                "max-output-file-size",
                "max-auto-compactions"
            })
    void disabledCompactionStillParsesEverySettingAndRetainsFlinksInvalidConfigurationErrors(String suffix) {
        var config = configured(0);
        String key = "state.backend.rocksdb.manual-compaction." + suffix;
        config.setString(key, "invalid");
        assertThatThrownBy(() -> RocksDBManualCompactionConfig.from(config))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                .contains("invalid Flink RocksDB configuration", key);
    }

    private static Configuration configured(long nanos) {
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBManualCompactionOptions.MIN_INTERVAL, Duration.ofNanos(nanos));
        config.set(RocksDBManualCompactionOptions.MAX_PARALLEL_COMPACTIONS, 2);
        config.set(RocksDBManualCompactionOptions.MAX_FILE_SIZE_TO_COMPACT, new MemorySize(100 << 10));
        config.set(RocksDBManualCompactionOptions.MIN_FILES_TO_COMPACT, 3);
        config.set(RocksDBManualCompactionOptions.MAX_FILES_TO_COMPACT, 20);
        config.set(RocksDBManualCompactionOptions.MAX_OUTPUT_FILE_SIZE, MemorySize.ofMebiBytes(32));
        config.set(RocksDBManualCompactionOptions.MAX_AUTO_COMPACTIONS, 2);
        return config;
    }
}
