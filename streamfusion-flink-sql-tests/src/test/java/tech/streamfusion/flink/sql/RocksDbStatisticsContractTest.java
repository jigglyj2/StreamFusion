/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricMonitor;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.apache.flink.state.rocksdb.RocksDBWriteBatchWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

/** The actual upstream monitor is the authority for native ticker names, types and lifecycle. */
class RocksDbStatisticsContractTest {
    @TempDir
    Path temporary;

    @Test
    void upstreamTickerGaugesMatchSharedNamesAndOnlyChangeOnViewUpdate() throws Exception {
        RocksDB.loadLibrary();
        var enabled = new RocksDBNativeMetricOptions();
        for (var option : List.of(
                RocksDBNativeMetricOptions.MONITOR_BLOCK_CACHE_HIT,
                RocksDBNativeMetricOptions.MONITOR_BLOCK_CACHE_MISS,
                RocksDBNativeMetricOptions.MONITOR_BLOOM_FILTER_USEFUL,
                RocksDBNativeMetricOptions.MONITOR_BLOOM_FILTER_FULL_POSITIVE,
                RocksDBNativeMetricOptions.MONITOR_BLOOM_FILTER_FULL_TRUE_POSITIVE,
                RocksDBNativeMetricOptions.MONITOR_BYTES_READ,
                RocksDBNativeMetricOptions.MONITOR_ITER_BYTES_READ,
                RocksDBNativeMetricOptions.MONITOR_BYTES_WRITTEN,
                RocksDBNativeMetricOptions.MONITOR_COMPACTION_READ_BYTES,
                RocksDBNativeMetricOptions.MONITOR_COMPACTION_WRITE_BYTES,
                RocksDBNativeMetricOptions.MONITOR_STALL_MICROS)) {
            enabled.enableNativeStatistics(option);
        }
        Map<String, Gauge<?>> gauges = new LinkedHashMap<>();
        var group = mock(MetricGroup.class);
        doAnswer(invocation -> {
                    String name = invocation.getArgument(0);
                    Gauge<?> gauge = invocation.getArgument(1);
                    assertThat(gauges.put(name, gauge)).isNull();
                    return gauge;
                })
                .when(group)
                .gauge(anyString(), any(Gauge.class));
        try (var statistics = new Statistics();
                var options = new Options().setCreateIfMissing(true).setStatistics(statistics);
                var db = RocksDB.open(options, temporary.resolve("metrics").toString())) {
            var monitor = new RocksDBNativeMetricMonitor(enabled, group, db, options.statistics());
            try {
                assertThat(gauges.keySet())
                        .containsExactlyInAnyOrderElementsOf(Files.readAllLines(Path.of(
                                "../streamfusion-state-rocksdb/tests/fixtures/flink-rocksdb-ticker-names.txt")));
                for (var gauge : gauges.values()) {
                    assertThat(gauge).isInstanceOf(View.class);
                    assertThat(gauge.getValue()).isEqualTo(0L);
                }
                db.put(new byte[] {1}, new byte[127]);
                assertThat(gauges.get("rocksdb.bytes_written").getValue()).isEqualTo(0L);
                gauges.values().forEach(gauge -> ((View) gauge).update());
                for (var ticker : enabled.getMonitorTickerTypes()) {
                    assertThat(gauges.get("rocksdb." + ticker.name().toLowerCase(java.util.Locale.ROOT))
                                    .getValue())
                            .isEqualTo(statistics.getTickerCount(ticker));
                }
                Object cached = gauges.get("rocksdb.bytes_written").getValue();
                assertThat((Long) cached).isPositive();
                monitor.close();
                db.put(new byte[] {2}, new byte[251]);
                gauges.values().forEach(gauge -> ((View) gauge).update());
                assertThat(gauges.get("rocksdb.bytes_written").getValue()).isEqualTo(cached);
            } finally {
                monitor.close();
            }
        }
    }

    @Test
    void generatedBulkReadUsesUpstreamsSeparateMultiGetTicker() throws Exception {
        RocksDB.loadLibrary();
        try (var statistics = new Statistics();
                var options = new Options().setCreateIfMissing(true).setStatistics(statistics);
                var db = RocksDB.open(options, temporary.resolve("reads").toString());
                var handle = db.getDefaultColumnFamily()) {
            var keys = new java.util.ArrayList<byte[]>();
            var expected = new java.util.ArrayList<byte[]>();
            try (var writes = new RocksDBWriteBatchWrapper(db, 4096)) {
                for (int index = 0; index < 617; index++) {
                    byte[] key = java.nio.ByteBuffer.allocate(8)
                            .putInt(index % 2)
                            .putInt(index)
                            .array();
                    byte[] value = new byte[index % 251 + 1];
                    java.util.Arrays.fill(value, (byte) index);
                    keys.add(key);
                    expected.add(value);
                    writes.put(handle, key, value);
                }
            }
            assertThat(db.multiGetAsList(keys)).containsExactlyElementsOf(expected);
            assertThat(statistics.getTickerCount(TickerType.BYTES_READ)).isZero();
            assertThat(statistics.getTickerCount(TickerType.NUMBER_MULTIGET_BYTES_READ))
                    .isEqualTo(
                            expected.stream().mapToLong(value -> value.length).sum());
            assertThat(db.get(keys.get(0))).isEqualTo(expected.get(0));
            assertThat(statistics.getTickerCount(TickerType.BYTES_READ)).isEqualTo(expected.get(0).length);
        }
    }
}
