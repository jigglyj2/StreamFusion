/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import tech.streamfusion.proto.plan.v1.NativeRocksDbOptions;
import tech.streamfusion.proto.plan.v1.NativeStateBinding;

/** Flink RocksDBResourceContainer settings, kept independent of the shared memory lease. */
final class NativeRocksDbConfiguration {
    private static final String OPTIONS = "org.apache.flink.state.rocksdb.RocksDBConfigurableOptions";
    private static final Set<String> FIELDS = Set.of(
            "MAX_BACKGROUND_THREADS",
            "MAX_OPEN_FILES",
            "LOG_MAX_FILE_SIZE",
            "LOG_FILE_NUM",
            "USE_DYNAMIC_LEVEL_SIZE",
            "TARGET_FILE_SIZE_BASE",
            "MAX_SIZE_LEVEL_BASE",
            "WRITE_BUFFER_SIZE",
            "WRITE_BATCH_SIZE",
            "MAX_WRITE_BUFFER_NUMBER",
            "MIN_WRITE_BUFFER_NUMBER_TO_MERGE",
            "COMPACT_FILTER_PERIODIC_COMPACTION_TIME",
            "BLOCK_SIZE",
            "METADATA_BLOCK_SIZE",
            "LOG_LEVEL",
            "LOG_DIR",
            "COMPACTION_STYLE",
            "COMPRESSION_PER_LEVEL",
            "USE_BLOOM_FILTER",
            "BLOOM_FILTER_BITS_PER_KEY",
            "BLOOM_FILTER_BLOCK_BASED_MODE",
            "BLOCK_CACHE_SIZE");

    private NativeRocksDbConfiguration() {}

    static Set<String> propagatedKeys() throws ReflectiveOperationException {
        Set<String> keys = new HashSet<>();
        keys.add("state.backend.rocksdb.predefined-options");
        for (String field : FIELDS) keys.add(option(field).key());
        return keys;
    }

    static NativeRocksDbOptions fromConfig(ReadableConfig config) throws ReflectiveOperationException {
        return fromConfig(config, preset(config));
    }

    static org.apache.flink.configuration.Configuration explicitOptions(ReadableConfig source)
            throws ReflectiveOperationException {
        var explicit = new org.apache.flink.configuration.Configuration();
        for (String field : FIELDS) copyExplicit(source, explicit, option(field));
        return explicit;
    }

    static NativeRocksDbOptions fromConfig(ReadableConfig config, String preset) throws ReflectiveOperationException {
        // Flink PredefinedOptions defaults, followed by explicit typed overrides below.
        var effective = new org.apache.flink.configuration.Configuration();
        if (!preset.equals("DEFAULT")
                && !preset.equals("FLASH_SSD_OPTIMIZED")
                && !preset.equals("SPINNING_DISK_OPTIMIZED")
                && !preset.equals("SPINNING_DISK_OPTIMIZED_HIGH_MEM")) {
            throw new IllegalArgumentException("Invalid state.backend.rocksdb.predefined-options: " + preset);
        }
        if (!preset.equals("DEFAULT")) {
            effective.setString(option("MAX_BACKGROUND_THREADS").key(), "4");
            effective.setString(option("MAX_OPEN_FILES").key(), "-1");
        }
        if (preset.startsWith("SPINNING_DISK_OPTIMIZED")) {
            effective.setString(option("USE_DYNAMIC_LEVEL_SIZE").key(), "true");
        }
        if (preset.equals("SPINNING_DISK_OPTIMIZED_HIGH_MEM")) {
            effective.setString(option("BLOCK_CACHE_SIZE").key(), "256 mb");
            effective.setString(option("BLOCK_SIZE").key(), "128 kb");
            effective.setString(option("MAX_SIZE_LEVEL_BASE").key(), "1 gb");
            effective.setString(option("MAX_WRITE_BUFFER_NUMBER").key(), "4");
            effective.setString(option("MIN_WRITE_BUFFER_NUMBER_TO_MERGE").key(), "3");
            effective.setString(option("TARGET_FILE_SIZE_BASE").key(), "256 mb");
            effective.setString(option("WRITE_BUFFER_SIZE").key(), "64 mb");
            effective.setString(option("USE_BLOOM_FILTER").key(), "true");
        }
        // ReadableConfig need not be a Configuration. Preserve typed parsing and aliases and
        // distinguish an explicitly configured default from an absent value.
        for (String field : FIELDS) copyExplicit(config, effective, option(field));
        config = effective;
        // Flink validates this setting even though its managed shared cache overrides it.
        size(config, "BLOCK_CACHE_SIZE");
        var builder = NativeRocksDbOptions.newBuilder()
                .setMaxBackgroundJobs(integer(config, "MAX_BACKGROUND_THREADS"))
                .setMaxOpenFiles(integer(config, "MAX_OPEN_FILES"))
                .setMaxLogFileSize(size(config, "LOG_MAX_FILE_SIZE"))
                .setKeepLogFileNum(integer(config, "LOG_FILE_NUM"))
                .setDynamicLevelBytes(booleanValue(config, "USE_DYNAMIC_LEVEL_SIZE"))
                .setTargetFileSizeBase(size(config, "TARGET_FILE_SIZE_BASE"))
                .setMaxBytesForLevelBase(size(config, "MAX_SIZE_LEVEL_BASE"))
                .setWriteBufferSize(size(config, "WRITE_BUFFER_SIZE"))
                .setWriteBatchSize(size(config, "WRITE_BATCH_SIZE"))
                .setMaxWriteBufferNumber(integer(config, "MAX_WRITE_BUFFER_NUMBER"))
                .setMinWriteBufferNumberToMerge(integer(config, "MIN_WRITE_BUFFER_NUMBER_TO_MERGE"))
                .setPeriodicCompactionSeconds(duration(config, "COMPACT_FILTER_PERIODIC_COMPACTION_TIME"))
                .setBlockSize(size(config, "BLOCK_SIZE"))
                .setMetadataBlockSize(size(config, "METADATA_BLOCK_SIZE"));
        String logDirectory = (String) config.get(option("LOG_DIR"));
        if (logDirectory != null) {
            if (!new java.io.File(logDirectory).isAbsolute() || logDirectory.indexOf(0) >= 0) {
                throw invalid("LOG_DIR");
            }
            builder.setLogDirectory(logDirectory);
        }
        return NativeRocksDbTableConfiguration.apply(config, builder).build();
    }

    private static <T> void copyExplicit(
            ReadableConfig source, org.apache.flink.configuration.Configuration target, ConfigOption<T> option) {
        source.getOptional(option).ifPresent(value -> target.set(option, value));
    }

    private static String preset(ReadableConfig config) throws ReflectiveOperationException {
        var option = (ConfigOption<?>) Class.forName(
                        "org.apache.flink.state.rocksdb.RocksDBOptions",
                        false,
                        Thread.currentThread().getContextClassLoader())
                .getField("PREDEFINED_OPTIONS")
                .get(null);
        return (String) config.get(option);
    }

    static ConfigOption<?> option(String field) throws ReflectiveOperationException {
        return (ConfigOption<?>)
                Class.forName(OPTIONS, false, Thread.currentThread().getContextClassLoader())
                        .getField(field)
                        .get(null);
    }

    private static int integer(ReadableConfig config, String field) throws ReflectiveOperationException {
        int value = (Integer) config.get(option(field));
        if (!field.equals("MAX_OPEN_FILES") && value <= 0) throw invalid(field);
        return value;
    }

    private static long size(ReadableConfig config, String field) throws ReflectiveOperationException {
        long value = ((MemorySize) config.get(option(field))).getBytes();
        if (value < 0 || (value == 0 && !field.equals("LOG_MAX_FILE_SIZE") && !field.equals("WRITE_BATCH_SIZE")))
            throw invalid(field);
        return value;
    }

    private static boolean booleanValue(ReadableConfig config, String field) throws ReflectiveOperationException {
        return (Boolean) config.get(option(field));
    }

    private static long duration(ReadableConfig config, String field) throws ReflectiveOperationException {
        long seconds = ((Duration) config.get(option(field))).getSeconds();
        if (seconds < 0) throw invalid(field);
        return seconds;
    }

    private static IllegalArgumentException invalid(String field) throws ReflectiveOperationException {
        return new IllegalArgumentException("Invalid value for " + option(field).key());
    }

    static NativeStateBinding bind(NativeStateBinding binding, NativeRocksDbOptions options) {
        if (!binding.hasRocksdb() || options == null) return binding;
        return binding.toBuilder()
                .setRocksdb(binding.getRocksdb().toBuilder().setDatabaseOptions(options))
                .build();
    }
}
