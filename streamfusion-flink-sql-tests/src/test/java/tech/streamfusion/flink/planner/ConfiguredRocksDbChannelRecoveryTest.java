/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

/** Reuses the Flink channel-state oracle for both checkpoint modes and both state backends. */
class ConfiguredRocksDbChannelRecoveryTest extends SharedAggregateChannelRecoveryTest {
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path logRoot;

    @Override
    protected org.apache.flink.configuration.Configuration backendOptions() {
        var options = RocksDbConfigurationProfiles.indexOptions(2);
        options.addAll(RocksDbStatisticsProfiles.allTickers());
        options.set(org.apache.flink.state.rocksdb.RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM, 2);
        options.set(
                org.apache.flink.state.rocksdb.RocksDBOptions.LOCAL_DIRECTORIES,
                logRoot.resolve("state-a") + "," + logRoot.resolve("state-b"));
        options.set(
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.LOG_DIR,
                logRoot.resolve("rocks-logs").toString());
        options.set(
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL,
                java.util.List.of(
                        org.rocksdb.CompressionType.LZ4_COMPRESSION, org.rocksdb.CompressionType.ZSTD_COMPRESSION));
        options.set(
                org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.WRITE_BATCH_SIZE,
                new org.apache.flink.configuration.MemorySize(50));
        options.set(org.apache.flink.state.rocksdb.RocksDBOptions.WRITE_BUFFER_RATIO, 0.7);
        options.set(org.apache.flink.state.rocksdb.RocksDBOptions.HIGH_PRIORITY_POOL_RATIO, 0.2);
        return options;
    }
}
