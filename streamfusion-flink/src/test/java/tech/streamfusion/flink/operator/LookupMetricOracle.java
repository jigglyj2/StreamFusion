/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;

/** Registers the gauges and advances I/O that Flink's surrounding task supplies to a lookup. */
final class LookupMetricOracle {
    private final OperatorMetricGroup flink;
    private final OperatorMetricGroup nativeStage;
    private final WatermarkGauge input = new WatermarkGauge();
    private final WatermarkGauge output = new WatermarkGauge();

    LookupMetricOracle(OperatorMetricGroup flink, OperatorMetricGroup nativeStage) {
        this.flink = flink;
        this.nativeStage = nativeStage;
        flink.gauge("currentInputWatermark", input);
        flink.gauge("currentOutputWatermark", output);
    }

    void records(long in, long out) throws Exception {
        flink.getIOMetricGroup().getNumRecordsInCounter().inc(in);
        flink.getIOMetricGroup().getNumRecordsOutCounter().inc(out);
        compare();
    }

    void watermark(long timestamp) throws Exception {
        input.setCurrentWatermark(timestamp);
        output.setCurrentWatermark(timestamp);
        compare();
    }

    void compare() throws Exception {
        var expected = registered(flink);
        var actual = registered(nativeStage);
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        assertThat(nativeStage.getAllVariables().keySet())
                .containsExactlyInAnyOrderElementsOf(flink.getAllVariables().keySet());
        for (var entry : expected.entrySet()) {
            var observed = actual.get(entry.getKey());
            assertThat(observed.getMetricType())
                    .as(entry.getKey())
                    .isEqualTo(entry.getValue().getMetricType());
            if (observed instanceof Counter) {
                assertThat(((Counter) observed).getCount())
                        .as(entry.getKey())
                        .isEqualTo(((Counter) entry.getValue()).getCount());
            } else if (observed instanceof Meter) {
                var meter = (Meter) observed;
                var reference = (Meter) entry.getValue();
                // Both are Flink's time-windowed meter implementation over the logical counter.
                // Wall-clock rates need not be equal; counts and the sampling definition must be.
                assertThat(meter.getClass()).isEqualTo(reference.getClass());
                assertThat(meter.getCount()).isEqualTo(reference.getCount());
                assertThat(meter.getRate()).isFinite().isGreaterThanOrEqualTo(0);
            } else if (observed instanceof Gauge) {
                assertThat(((Gauge<?>) observed).getValue())
                        .as(entry.getKey())
                        .isEqualTo(((Gauge<?>) entry.getValue()).getValue());
            } else throw new AssertionError("Unverified lookup metric " + entry.getKey());
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Metric> registered(OperatorMetricGroup group) throws Exception {
        var field = AbstractMetricGroup.class.getDeclaredField("metrics");
        field.setAccessible(true);
        return Map.copyOf((Map<String, Metric>) field.get(group));
    }
}
