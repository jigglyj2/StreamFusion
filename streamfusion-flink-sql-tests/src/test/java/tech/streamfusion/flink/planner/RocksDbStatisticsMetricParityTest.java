/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.View;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class RocksDbStatisticsMetricParityTest {
    @Test
    void selectedRegionMatchesFullFlinkSurfaceAndSamplesActualStorageWorkUntilClose() throws Exception {
        var options = RocksDbStatisticsProfiles.allTickers();
        for (boolean rocks : List.of(false, true)) {
            Map<String, Metric> expected;
            Map<String, Metric> actual;
            Map<String, Object> expectedClosed;
            Map<String, Object> actualClosed;
            try (var oracle = SharedAggregateFlinkOracle.create(rocks, 0, true, options, 16L << 20);
                    var nativeHarness = SharedAggregateRuntimeHarness.configured(rocks, null, options, 16L << 20);
                    var allocator = new RootAllocator(64L << 20)) {
                var flinkGroup = oracle.getOperator().getMetricGroup();
                flinkGroup.gauge(
                        "currentInputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                flinkGroup.gauge(
                        "currentOutputWatermark", new org.apache.flink.streaming.runtime.metrics.WatermarkGauge());
                expected = RegisteredMetricSurface.metrics(flinkGroup);
                actual = RegisteredMetricSurface.metrics(SharedAggregateMetricSurfaceTest.stageGroup(nativeHarness, 3));
                RegisteredMetricSurface.compare(expected, actual);
                assertThat(tickers(actual)).hasSize(rocks ? 11 : 0);
                var rows = new ArrayList<GenericRowData>();
                for (int index = 0; index < 257; index++) {
                    var row = GenericRowData.of(
                            index % 17 == 0 ? null : StringData.fromString("é-" + index % 7), (long) index);
                    rows.add(row);
                    flinkGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
                    oracle.processElement(new StreamRecord<>(row));
                }
                flinkGroup
                        .getIOMetricGroup()
                        .getNumRecordsOutCounter()
                        .inc(oracle.extractOutputStreamRecords().size());
                var changelog = new HashMap<String, List<String>>();
                for (var record : oracle.extractOutputStreamRecords())
                    SharedAggregateRuntimeHarness.record(changelog, record.getValue(), null);
                try (var batch = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)) {
                    nativeHarness.processElement(0, new StreamRecord<>(batch));
                }
                assertThat(nativeHarness.changelog).containsExactlyInAnyOrderEntriesOf(changelog);
                // All ticker getters remain cached until the Flink view updater samples them.
                assertThat(tickers(expected).values())
                        .allSatisfy(value -> assertThat(value).isEqualTo(0L));
                assertThat(tickers(actual).values())
                        .allSatisfy(value -> assertThat(value).isEqualTo(0L));
                update(expected);
                update(actual);
                assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
                for (String name : tickers(expected).keySet()) {
                    assertThat(actual.get(name).getMetricType())
                            .isEqualTo(expected.get(name).getMetricType());
                    assertThat(actual.get(name)).isInstanceOf(View.class);
                    assertThat(tickers(actual).get(name)).isInstanceOf(Long.class);
                    assertThat((Long) tickers(actual).get(name)).isGreaterThanOrEqualTo(0L);
                }
                if (rocks) {
                    assertThat((Long) tickers(expected).get("rocksdb.bytes_written"))
                            .isPositive();
                    assertThat((Long) tickers(actual).get("rocksdb.bytes_written"))
                            .isPositive();
                    // Flink issues Get; the native batch uses MultiGet. These are actual physical
                    // definitions, not fabricated equal byte counts for different storage work.
                    assertThat((Long) tickers(expected).get("rocksdb.bytes_read"))
                            .isPositive();
                    assertThat(tickers(actual).get("rocksdb.bytes_read")).isEqualTo(0L);
                }
                var expectedLogical = new HashMap<>(expected);
                var actualLogical = new HashMap<>(actual);
                tickers(expected).keySet().forEach(expectedLogical::remove);
                tickers(actual).keySet().forEach(actualLogical::remove);
                RegisteredMetricSurface.compare(expectedLogical, actualLogical);
                expectedClosed = tickers(expected);
                actualClosed = tickers(actual);
            }
            update(expected);
            update(actual);
            assertThat(tickers(expected)).isEqualTo(expectedClosed);
            assertThat(tickers(actual)).isEqualTo(actualClosed);
        }
    }

    private static Map<String, Object> tickers(Map<String, Metric> metrics) {
        var values = new HashMap<String, Object>();
        metrics.forEach((name, metric) -> {
            if (name.startsWith("rocksdb.")) values.put(name, ((Gauge<?>) metric).getValue());
        });
        return values;
    }

    private static void update(Map<String, Metric> metrics) {
        metrics.forEach((name, metric) -> {
            if (name.startsWith("rocksdb.")) ((View) metric).update();
        });
    }
}
