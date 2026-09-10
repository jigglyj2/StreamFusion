/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

/** Flink's real WindowJoin metrics and test processing clock provide the semantic oracle. */
final class WindowJoinRegionMetrics {
    private WindowJoinRegionMetrics() {}

    static void compare(
            WindowJoinRegionFixture nativeHarness,
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> flink)
            throws Exception {
        var stage = NativeRegionTestHarness.stageMetrics(nativeHarness.region(), 3);
        assertThat(stage.getIOMetricGroup().getNumRecordsInCounter().getCount()).isEqualTo(nativeHarness.acceptedRows);
        assertThat(stage.getIOMetricGroup().getNumRecordsOutCounter().getCount())
                .isEqualTo(nativeHarness.emittedRows);
        var calc =
                NativeRegionTestHarness.stageMetrics(nativeHarness.region(), 4).getIOMetricGroup();
        assertThat(calc.getNumRecordsInCounter().getCount()).isEqualTo(nativeHarness.emittedRows);
        assertThat(calc.getNumRecordsOutCounter().getCount()).isEqualTo(nativeHarness.emittedRows);
        var actual = registered(stage);
        var expected = registered(flink.getOperator().getMetricGroup());
        for (String side : new String[] {"left", "right"}) {
            String counter = side + "NumLateRecordsDropped";
            String meter = side + "LateRecordsDroppedRate";
            assertThat(actual.get(counter)).isInstanceOf(Counter.class);
            assertThat(((Counter) actual.get(counter)).getCount())
                    .isEqualTo(((Counter) expected.get(counter)).getCount());
            assertThat(actual.get(meter)).isInstanceOf(MeterView.class);
            var actualMeter = (MeterView) actual.get(meter);
            var expectedMeter = (MeterView) expected.get(meter);
            actualMeter.update();
            expectedMeter.update();
            assertThat(actualMeter.getCount()).isEqualTo(expectedMeter.getCount());
            assertThat(actualMeter.getRate()).isEqualTo(expectedMeter.getRate());
        }
        assertThat(actual.get("watermarkLatency")).isInstanceOf(Gauge.class);
        assertThat(((Gauge<?>) actual.get("watermarkLatency")).getValue())
                .isEqualTo(((Gauge<?>) expected.get("watermarkLatency")).getValue());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Metric> registered(Object group) throws Exception {
        var field = AbstractMetricGroup.class.getDeclaredField("metrics");
        field.setAccessible(true);
        return (Map<String, Metric>) field.get(group);
    }
}
