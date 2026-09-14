/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;

/** Deterministic nondefault settings shared by SQL, metric, and recovery parity tests. */
public final class RocksDbConfigurationProfiles {
    private RocksDbConfigurationProfiles() {}

    public static Configuration databaseOptions(int seed) {
        var config = new Configuration();
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
        return config;
    }

    public static Configuration presetOptions(String preset, boolean override) {
        var config = databaseOptions(1);
        config.set(org.apache.flink.state.rocksdb.RocksDBOptions.PREDEFINED_OPTIONS, preset);
        if (!override) {
            config.removeConfig(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS);
            config.removeConfig(RocksDBConfigurableOptions.MAX_OPEN_FILES);
            config.removeConfig(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE);
        }
        return config;
    }

    public static Configuration tableOptions(int profile) {
        var config = profile == 0 ? new Configuration() : databaseOptions(profile);
        if (profile == 0) {
            config.set(
                    org.apache.flink.state.rocksdb.RocksDBOptions.PREDEFINED_OPTIONS,
                    "SPINNING_DISK_OPTIMIZED_HIGH_MEM");
            return config;
        }
        var none = org.rocksdb.CompressionType.NO_COMPRESSION;
        var snappy = org.rocksdb.CompressionType.SNAPPY_COMPRESSION;
        config.set(
                RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL,
                profile == 1
                        ? java.util.List.of(none, snappy)
                        : profile == 2 ? java.util.List.of(none) : java.util.List.of());
        config.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, profile != 2);
        config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY, profile == 1 ? 9.9 : 100.0);
        config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE, profile == 3);
        config.set(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, MemorySize.ofMebiBytes(1));
        config.set(
                RocksDBConfigurableOptions.LOG_LEVEL,
                profile == 1 ? org.rocksdb.InfoLogLevel.DEBUG_LEVEL : org.rocksdb.InfoLogLevel.WARN_LEVEL);
        return config;
    }

    public static Configuration indexOptions(int profile) {
        var config = tableOptions(profile % 4);
        config.set(org.apache.flink.state.rocksdb.RocksDBOptions.USE_PARTITIONED_INDEX_FILTERS, profile < 3);
        config.set(
                RocksDBConfigurableOptions.COMPACTION_STYLE,
                profile == 0
                        ? org.rocksdb.CompactionStyle.LEVEL
                        : profile % 2 == 1 ? org.rocksdb.CompactionStyle.UNIVERSAL : org.rocksdb.CompactionStyle.NONE);
        config.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, profile > 0 && profile < 4);
        config.set(RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY, profile == 2 ? 0.0 : 1.0);
        return config;
    }
}
