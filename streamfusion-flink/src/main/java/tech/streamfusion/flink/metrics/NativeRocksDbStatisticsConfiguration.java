/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.configuration.ReadableConfig;

/** Resolves optional RocksDB metric settings with Flink's own typed option parser. */
public final class NativeRocksDbStatisticsConfiguration {
    private static final List<String> TICKERS = List.of(
            "BLOCK_CACHE_HIT",
            "BLOCK_CACHE_MISS",
            "BLOOM_FILTER_USEFUL",
            "BLOOM_FILTER_FULL_POSITIVE",
            "BLOOM_FILTER_FULL_TRUE_POSITIVE",
            "BYTES_READ",
            "ITER_BYTES_READ",
            "BYTES_WRITTEN",
            "COMPACT_READ_BYTES",
            "COMPACT_WRITE_BYTES",
            "STALL_MICROS");

    private NativeRocksDbStatisticsConfiguration() {}

    public static List<Integer> fromConfig(ReadableConfig config) throws ReflectiveOperationException {
        final Class<?> type;
        try {
            type = Class.forName(
                    "org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions",
                    false,
                    Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException absentOptionalBackend) {
            return List.of();
        }
        Object resolved = type.getMethod("fromConfig", ReadableConfig.class).invoke(null, config);
        if (!((Collection<?>) type.getMethod("getProperties").invoke(resolved)).isEmpty()) {
            throw new UnsupportedOperationException(
                    "metrics: enabled RocksDB column-family property metrics are not yet published by shared native state");
        }
        var selected = new ArrayList<Integer>();
        for (Object ticker :
                (Collection<?>) type.getMethod("getMonitorTickerTypes").invoke(resolved)) {
            String name = ((Enum<?>) ticker).name();
            int code = TICKERS.indexOf(name);
            if (code < 0) throw new UnsupportedOperationException("metrics: unsupported Flink RocksDB ticker " + name);
            selected.add(code);
        }
        selected.sort(Integer::compareTo);
        return List.copyOf(selected);
    }
}
