/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;

class NativeRocksDbTableConfigurationTest {
    @Test
    void generatedFiltersCompressionAndLogLevelsMatchFlinksActualResourceContainer() throws Exception {
        RocksDB.loadLibrary();
        var bitsField = BloomFilter.class.getDeclaredField("bitsPerKey");
        bitsField.setAccessible(true);
        var codecLists = List.of(
                List.<CompressionType>of(),
                List.of(CompressionType.NO_COMPRESSION),
                List.of(CompressionType.SNAPPY_COMPRESSION),
                List.of(
                        CompressionType.NO_COMPRESSION,
                        CompressionType.SNAPPY_COMPRESSION,
                        CompressionType.NO_COMPRESSION));
        for (double bits : new double[] {-1, 0, 0.49, 0.5, 1, 9.9, 100, Double.POSITIVE_INFINITY, Double.NaN}) {
            for (int seed = 0; seed < 6; seed++) {
                var config = new Configuration();
                config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
                config.set(RocksDBConfigurableOptions.LOG_LEVEL, InfoLogLevel.values()[seed]);
                config.set(RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL, codecLists.get(seed % codecLists.size()));
                config.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, seed % 3 != 0);
                config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY, bits);
                config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE, seed % 2 == 0);
                assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                        .isNull();
                var resolved = NativeRocksDbConfiguration.fromConfig(config);
                try (var flink =
                        new RocksDBResourceContainer(config, PredefinedOptions.DEFAULT, null, null, null, false)) {
                    var cf = flink.getColumnOptions();
                    var table = (BlockBasedTableConfig) cf.tableFormatConfig();
                    assertThat(resolved.getLogLevel())
                            .isEqualTo(flink.getDbOptions().infoLogLevel().getValue());
                    assertThat(resolved.getCompression().getPerLevelList())
                            .containsExactlyElementsOf(cf.compressionPerLevel().stream()
                                    .map(codec -> codec == CompressionType.NO_COMPRESSION ? 0 : 1)
                                    .collect(java.util.stream.Collectors.toList()));
                    assertThat(resolved.getBloomFilter().getEnabled()).isEqualTo(table.filterPolicy() != null);
                    assertThat(resolved.getBloomFilter().getBlockBasedMode()).isEqualTo(seed % 2 == 0);
                    if (table.filterPolicy() != null) {
                        assertThat(Double.doubleToLongBits(
                                        resolved.getBloomFilter().getBitsPerKey()))
                                .isEqualTo(Double.doubleToLongBits(bitsField.getDouble(table.filterPolicy())));
                    }
                }
            }
        }
    }

    @Test
    void highMemoryPresetPreservesAllItsTableSettingsAndExplicitOverrides() throws Exception {
        RocksDB.loadLibrary();
        for (boolean override : new boolean[] {false, true}) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            config.set(RocksDBOptions.PREDEFINED_OPTIONS, "SPINNING_DISK_OPTIMIZED_HIGH_MEM");
            if (override) {
                config.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, false);
                config.set(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, MemorySize.ofMebiBytes(1));
                config.set(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, 2);
                config.set(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 1);
                config.set(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, MemorySize.ofMebiBytes(64));
            }
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
            var resolved = NativeRocksDbConfiguration.fromConfig(config);
            try (var flink = new RocksDBResourceContainer(
                    config, PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM, null, null, null, false)) {
                var cf = flink.getColumnOptions();
                var table = (BlockBasedTableConfig) cf.tableFormatConfig();
                assertThat(resolved.getMaxBackgroundJobs())
                        .isEqualTo(flink.getDbOptions().maxBackgroundJobs());
                assertThat(resolved.getMaxOpenFiles())
                        .isEqualTo(flink.getDbOptions().maxOpenFiles());
                assertThat(resolved.getBlockSize()).isEqualTo(table.blockSize());
                assertThat(resolved.getDynamicLevelBytes()).isEqualTo(cf.levelCompactionDynamicLevelBytes());
                assertThat(resolved.getMaxBytesForLevelBase()).isEqualTo(cf.maxBytesForLevelBase());
                assertThat(resolved.getMaxWriteBufferNumber()).isEqualTo(cf.maxWriteBufferNumber());
                assertThat(resolved.getMinWriteBufferNumberToMerge()).isEqualTo(cf.minWriteBufferNumberToMerge());
                assertThat(resolved.getTargetFileSizeBase()).isEqualTo(cf.targetFileSizeBase());
                assertThat(resolved.getWriteBufferSize()).isEqualTo(cf.writeBufferSize());
                assertThat(resolved.getBloomFilter().getEnabled()).isEqualTo(table.filterPolicy() != null);
            }
        }
    }

    @Test
    void uncompiledCodecsAndInvalidManagedCacheSizesRetainPreciseFallback() {
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        for (var codec : CompressionType.values()) {
            if (codec != CompressionType.XPRESS_COMPRESSION && codec != CompressionType.DISABLE_COMPRESSION_OPTION)
                continue;
            config.set(RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL, List.of(codec));
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .contains("compression.per.level", codec.name());
        }
        config.removeConfig(RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL);
        config.set(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, MemorySize.ZERO);
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                .contains(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE.key());
    }

    @Test
    void flinkManagedMemoryOverridesBothExplicitAndPresetBlockCacheSizes(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path temporary) throws Exception {
        RocksDB.loadLibrary();
        for (int size : new int[] {1, 256}) {
            var config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            config.set(RocksDBOptions.PREDEFINED_OPTIONS, "SPINNING_DISK_OPTIMIZED_HIGH_MEM");
            config.set(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, MemorySize.ofMebiBytes(size));
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
            long budget = 16L << 20;
            var resources = org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                    budget,
                    0.5,
                    0.1,
                    false,
                    org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT);
            try (var lease = lease(resources, budget);
                    var flink = new RocksDBResourceContainer(
                            config, PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM, null, lease, null, false)) {
                var handles = new java.util.ArrayList<org.rocksdb.ColumnFamilyHandle>();
                try (var db = RocksDB.open(
                        flink.getDbOptions(),
                        temporary.resolve("db-" + size).toString(),
                        List.of(new org.rocksdb.ColumnFamilyDescriptor(
                                RocksDB.DEFAULT_COLUMN_FAMILY, flink.getColumnOptions())),
                        handles)) {
                    try {
                        assertThat(db.getLongProperty("rocksdb.block-cache-capacity"))
                                .isEqualTo(
                                        org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils
                                                .calculateActualCacheCapacity(budget, 0.5));
                    } finally {
                        handles.forEach(org.rocksdb.ColumnFamilyHandle::close);
                    }
                }
            }
        }
    }

    private static <T extends AutoCloseable> org.apache.flink.runtime.memory.OpaqueMemoryResource<T> lease(
            T resource, long budget) {
        return new org.apache.flink.runtime.memory.OpaqueMemoryResource<>(resource, budget, resource::close);
    }
}
