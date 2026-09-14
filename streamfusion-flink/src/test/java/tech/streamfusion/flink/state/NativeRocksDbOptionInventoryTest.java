/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend.PriorityQueueStateType;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NativeRocksDbOptionInventoryTest {
    @Test
    void everyPublishedBackendOptionHasAnExplicitSupportDecision() throws Exception {
        var published = new HashSet<String>();
        for (Class<?> type : new Class<?>[] {
            RocksDBOptions.class, RocksDBConfigurableOptions.class, RocksDBManualCompactionOptions.class
        }) {
            for (var field : type.getFields())
                if (Modifier.isStatic(field.getModifiers()) && ConfigOption.class.isAssignableFrom(field.getType()))
                    published.add(((ConfigOption<?>) field.get(null)).key());
        }
        Set<String> propagated = NativeRocksDbConfiguration.propagatedKeys();
        for (String key : published) if (NativeRocksDbMemoryConfiguration.propagates(key)) propagated.add(key);
        propagated.add(NativeRocksDbStorageDirectories.key());
        propagated.add(NativeRocksDbTransfers.validatedConfigurationKey(new Configuration()));
        assertThat(propagated).doesNotContainAnyElementsOf(NativeRocksDbOptionSupport.defaultOnlyKeys());
        var audited = new HashSet<>(propagated);
        audited.addAll(NativeRocksDbOptionSupport.defaultOnlyKeys());
        assertThat(audited).doesNotContainAnyElementsOf(NativeRocksDbManualCompactionConfiguration.keys());
        audited.addAll(NativeRocksDbManualCompactionConfiguration.keys());
        assertThat(published).containsExactlyInAnyOrderElementsOf(audited);
        assertThat(defaultOnlyOptions().map(a -> ((ConfigOption<?>) a.get()[0]).key()))
                .containsExactlyInAnyOrderElementsOf(NativeRocksDbOptionSupport.defaultOnlyKeys());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("defaultOnlyOptions")
    void defaultOnlySettingsKeepTheirAuditedDefaultsAndExplainNonDefaultFallback(
            ConfigOption<?> option, Object expectedDefault, Object override, String reason) {
        assertThat(option.defaultValue()).as(option.key()).isEqualTo(expectedDefault);
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
        set(config, option, override);
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).contains(option.key(), reason);
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
    }

    @Test
    void anUnauditedUpstreamOptionRequiresFallbackEvenAtItsDefault() {
        var future = ConfigOptions.key("state.backend.rocksdb.test-future-option")
                .intType()
                .defaultValue(1);
        var config = new Configuration();
        for (int iteration = 0; iteration < 3; iteration++) {
            if (iteration != 0) config.set(future, iteration);
            assertThat(NativeRocksDbOptionSupport.unsupportedReason(config, future))
                    .contains("no audited support", future.key());
        }
    }

    @SuppressWarnings("unchecked")
    private static void set(Configuration config, ConfigOption<?> option, Object value) {
        config.set((ConfigOption<Object>) option, value);
    }

    static Stream<Arguments> defaultOnlyOptions() {
        return Stream.of(
                arguments(
                        RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE,
                        null,
                        MemorySize.ofMebiBytes(32),
                        "one fixed cache budget"),
                arguments(
                        RocksDBOptions.FIX_PER_TM_MEMORY_SIZE,
                        null,
                        MemorySize.ofMebiBytes(64),
                        "one fixed cache budget"),
                arguments(RocksDBOptions.USE_MANAGED_MEMORY, true, false, "managed STATE_BACKEND budget"),
                arguments(
                        RocksDBOptions.OPTIONS_FACTORY,
                        null,
                        "example.CustomFactory",
                        "RocksDBOptionsFactory callbacks"),
                arguments(
                        RocksDBOptions.TIMER_SERVICE_FACTORY,
                        PriorityQueueStateType.ROCKSDB,
                        PriorityQueueStateType.HEAP,
                        "timer-service parity"),
                arguments(
                        RocksDBOptions.ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE,
                        128,
                        256,
                        "per-key-group RocksDB timer cache"),
                arguments(
                        RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD,
                        0.0,
                        0.5,
                        "handle-overlap fraction"),
                arguments(
                        RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE, false, true, "exported column families"),
                arguments(
                        RocksDBConfigurableOptions.INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE,
                        false,
                        true,
                        "post-rescale range compaction"),
                arguments(
                        RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING,
                        false,
                        true,
                        "deleteFilesInRange"),
                arguments(
                        RocksDBConfigurableOptions.COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES,
                        1000L,
                        500L,
                        "timestamp-refresh cadence"));
    }
}
