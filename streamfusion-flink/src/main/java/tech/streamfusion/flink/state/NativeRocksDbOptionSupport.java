/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

/** Audited default-only options, separate from settings propagated to the native component. */
final class NativeRocksDbOptionSupport {
    private static final String FIXED_MEMORY =
            "one fixed cache budget cannot yet be shared across Java and native RocksDB owners";
    private static final Map<String, String> DEFAULT_ONLY = Map.ofEntries(
            Map.entry("state.backend.rocksdb.memory.fixed-per-slot", FIXED_MEMORY),
            Map.entry("state.backend.rocksdb.memory.fixed-per-tm", FIXED_MEMORY),
            Map.entry(
                    "state.backend.rocksdb.memory.managed",
                    "native state requires Flink's managed STATE_BACKEND budget"),
            Map.entry(
                    "state.backend.rocksdb.options-factory",
                    "custom RocksDBOptionsFactory callbacks are not translated into native database options"),
            Map.entry(
                    "state.backend.rocksdb.timer-service.factory",
                    "native timer-service parity has not been verified for the selected backend"),
            Map.entry(
                    "state.backend.rocksdb.timer-service.cache-size",
                    "native timers do not yet implement Flink's per-key-group RocksDB timer cache"),
            Map.entry(
                    "state.backend.rocksdb.restore-overlap-fraction-threshold",
                    "native restore does not select an initial database by handle-overlap fraction"),
            Map.entry(
                    "state.backend.rocksdb.use-ingest-db-restore-mode",
                    "native restore does not import exported column families"),
            Map.entry(
                    "state.backend.rocksdb.incremental-restore-async-compact-after-rescale",
                    "native restore does not schedule Flink's post-rescale range compaction"),
            Map.entry(
                    "state.backend.rocksdb.rescaling.use-delete-files-in-range",
                    "native restore does not call deleteFilesInRange while clipping key groups"),
            Map.entry(
                    "state.backend.rocksdb.compaction.filter.query-time-after-num-entries",
                    "native state does not install Flink's TTL compaction filter or its timestamp-refresh cadence"));

    private NativeRocksDbOptionSupport() {}

    static Set<String> defaultOnlyKeys() {
        return DEFAULT_ONLY.keySet();
    }

    static String unsupportedReason(ReadableConfig config, ConfigOption<?> option) {
        String detail = DEFAULT_ONLY.get(option.key());
        if (detail == null) {
            // A new upstream option may change behavior at its default value too. It needs an
            // explicit source audit before native state can claim support for this dependency.
            return "state backend: native RocksDB has no audited support for " + option.key();
        }
        if (Objects.equals(config.get(option), option.defaultValue())) return null;
        return "state backend: native RocksDB does not yet propagate " + option.key() + "; " + detail;
    }
}
