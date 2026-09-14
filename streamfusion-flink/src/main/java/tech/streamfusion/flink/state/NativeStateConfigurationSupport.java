/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

/** Rejects backend settings whose behavior has not been implemented by the native state component. */
public final class NativeStateConfigurationSupport {
    private NativeStateConfigurationSupport() {}

    public static String unsupportedReason(ReadableConfig config) {
        String backend = StreamFusionStateBackendFactory.configuredBackend(config);
        if (backend.equals("hashmap")) return null;
        if (!backend.equals("rocksdb")) {
            return "state backend: native keyed regions do not support configured backend " + backend;
        }
        return rocksDbUnsupportedReason(config);
    }

    static String rocksDbUnsupportedReason(ReadableConfig config) {
        try {
            // Resolve Flink's typed options (including deprecated aliases) instead of checking
            // only raw option names. Keep RocksDB optional for in-memory deployments.
            var options = new TreeMap<String, ConfigOption<?>>();
            for (String name : List.of(
                    "org.apache.flink.state.rocksdb.RocksDBOptions",
                    "org.apache.flink.state.rocksdb.RocksDBConfigurableOptions",
                    "org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionOptions")) {
                Class<?> type =
                        Class.forName(name, false, Thread.currentThread().getContextClassLoader());
                for (var field : type.getFields()) {
                    if (Modifier.isStatic(field.getModifiers())
                            && ConfigOption.class.isAssignableFrom(field.getType())) {
                        var option = (ConfigOption<?>) field.get(null);
                        options.put(option.key(), option);
                    }
                }
            }
            NativeRocksDbMemoryConfiguration.fromConfig(config);
            NativeRocksDbConfiguration.fromConfig(config);
            NativeRocksDbStorageDirectories.validate(config);
            NativeRocksDbManualCompactionConfiguration.validate(config);
            var propagated = NativeRocksDbConfiguration.propagatedKeys();
            propagated.add(NativeRocksDbStorageDirectories.key());
            propagated.add(NativeRocksDbTransfers.validatedConfigurationKey(config));
            for (var option : options.values()) {
                if (NativeRocksDbMemoryConfiguration.propagates(option.key())
                        || propagated.contains(option.key())
                        || NativeRocksDbManualCompactionConfiguration.keys().contains(option.key())) continue;
                String reason = NativeRocksDbOptionSupport.unsupportedReason(config, option);
                if (reason != null) return reason;
            }
        } catch (UnsupportedOperationException failure) {
            return "state backend: " + failure.getMessage();
        } catch (IllegalArgumentException failure) {
            return "state backend: invalid Flink RocksDB configuration: " + failure.getMessage();
        } catch (ReflectiveOperationException | LinkageError | RuntimeException failure) {
            return "state backend: cannot resolve Flink RocksDB configuration: "
                    + failure.getClass().getSimpleName();
        }
        return null;
    }
}
