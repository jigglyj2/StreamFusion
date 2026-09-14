/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.IndexType;
import org.rocksdb.RocksDB;

class NativeRocksDbPartitionConfigurationTest {
    @Test
    void sharedPartitioningMatchesFlinksIndexPolicyAndTenBitFilterOverride() throws Exception {
        RocksDB.loadLibrary();
        var bits = BloomFilter.class.getDeclaredField("bitsPerKey");
        bits.setAccessible(true);
        for (boolean partitioned : List.of(false, true)) {
            for (boolean bloom : List.of(false, true)) {
                for (double configuredBits : new double[] {0, 1, 9.9, 100}) {
                    var config = new Configuration();
                    config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
                    config.set(RocksDBOptions.USE_PARTITIONED_INDEX_FILTERS, partitioned);
                    config.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, bloom);
                    config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY, configuredBits);
                    config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE, true);
                    assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                            .isNull();
                    var memory = NativeRocksDbMemoryConfiguration.fromConfig(config);
                    var options = NativeRocksDbConfiguration.fromConfig(config);
                    var shared = RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                            16L << 20,
                            0.5,
                            0.1,
                            memory.partitionedIndexFilters(),
                            RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT);
                    try (var lease = lease(shared);
                            var flink = new RocksDBResourceContainer(
                                    config, PredefinedOptions.DEFAULT, null, lease, null, false)) {
                        var table =
                                (BlockBasedTableConfig) flink.getColumnOptions().tableFormatConfig();
                        assertThat(table.indexType())
                                .isEqualTo(
                                        memory.partitionedIndexFilters()
                                                ? IndexType.kTwoLevelIndexSearch
                                                : IndexType.kBinarySearch);
                        assertThat(table.partitionFilters()).isEqualTo(memory.partitionedIndexFilters());
                        // Both RocksDB versions default this to true, including when no partitioned index exists.
                        assertThat(table.pinTopLevelIndexAndFilter()).isTrue();
                        assertThat(table.cacheIndexAndFilterBlocks()).isTrue();
                        assertThat(table.cacheIndexAndFilterBlocksWithHighPriority())
                                .isTrue();
                        assertThat(table.pinL0FilterAndIndexBlocksInCache()).isTrue();
                        assertThat(table.filterPolicy() != null)
                                .isEqualTo(options.getBloomFilter().getEnabled());
                        if (bloom)
                            assertThat(bits.getDouble(table.filterPolicy()))
                                    .isEqualTo(
                                            memory.partitionedIndexFilters()
                                                    ? 10.0
                                                    : options.getBloomFilter().getBitsPerKey());
                    }
                }
            }
        }
    }

    @Test
    void supportedCompactionStylesMatchFlinkAndOverridePresets() throws Exception {
        RocksDB.loadLibrary();
        for (var preset : PredefinedOptions.values()) {
            for (var style : List.of(CompactionStyle.LEVEL, CompactionStyle.UNIVERSAL, CompactionStyle.NONE)) {
                var config = new Configuration();
                config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
                config.set(RocksDBOptions.PREDEFINED_OPTIONS, preset.name());
                config.set(RocksDBConfigurableOptions.COMPACTION_STYLE, style);
                assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                        .isNull();
                try (var flink = new RocksDBResourceContainer(config, preset, null, null, null, false)) {
                    assertThat(NativeRocksDbConfiguration.fromConfig(config).getCompactionStyle())
                            .isEqualTo(
                                    flink.getColumnOptions().compactionStyle().getValue());
                }
            }
        }
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBConfigurableOptions.COMPACTION_STYLE, CompactionStyle.FIFO);
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                .contains("compaction.style=FIFO", "state-eviction parity");
    }

    private static <T extends AutoCloseable> OpaqueMemoryResource<T> lease(T resource) {
        return new OpaqueMemoryResource<>(resource, 16L << 20, resource::close);
    }
}
