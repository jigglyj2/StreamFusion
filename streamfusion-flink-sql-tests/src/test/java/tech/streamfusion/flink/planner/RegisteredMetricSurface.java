/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

/** Discover the actual registered surface; unknown metric semantics fail rather than being skipped. */
final class RegisteredMetricSurface {
    private RegisteredMetricSurface() {}

    static void compare(Map<String, Metric> expected, Map<String, Metric> actual) {
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        for (var entry : expected.entrySet()) {
            var value = actual.get(entry.getKey());
            assertThat(value.getMetricType())
                    .as(entry.getKey())
                    .isEqualTo(entry.getValue().getMetricType());
            if (value instanceof Counter)
                assertThat(((Counter) value).getCount())
                        .as(entry.getKey())
                        .isEqualTo(((Counter) entry.getValue()).getCount());
            else if (value instanceof Gauge)
                assertThat(((Gauge<?>) value).getValue())
                        .as(entry.getKey())
                        .isEqualTo(((Gauge<?>) entry.getValue()).getValue());
            else if (value instanceof Meter) {
                assertThat(((Meter) value).getCount())
                        .as(entry.getKey())
                        .isEqualTo(((Meter) entry.getValue()).getCount());
                assertThat(((Meter) value).getRate()).isFinite().isGreaterThanOrEqualTo(0);
                assertThat(value.getClass())
                        .as("meter implementation/semantics")
                        .isEqualTo(entry.getValue().getClass());
            } else throw new AssertionError("Add semantic parity for newly registered metric " + entry.getKey());
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Metric> metrics(Object group) throws Exception {
        var metricsField = AbstractMetricGroup.class.getDeclaredField("metrics");
        var groupsField = AbstractMetricGroup.class.getDeclaredField("groups");
        metricsField.setAccessible(true);
        groupsField.setAccessible(true);
        var result = new HashMap<>((Map<String, Metric>) metricsField.get(group));
        for (var child : ((Map<String, ?>) groupsField.get(group)).entrySet())
            for (var metric : metrics(child.getValue()).entrySet())
                result.put(child.getKey() + "/" + metric.getKey(), metric.getValue());
        return result;
    }

    static Map<String, Metric> latency(Object taskGroup, OperatorID operator) throws Exception {
        var result = new HashMap<String, Metric>();
        for (var entry : metrics(taskGroup).entrySet())
            if (entry.getKey().startsWith("latency/") && entry.getKey().contains("/operator_id/" + operator + "/"))
                result.put(entry.getKey(), entry.getValue());
        return result;
    }

    static void compareLatency(Map<String, Metric> expected, Map<String, Metric> actual) {
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        for (var entry : expected.entrySet()) {
            assertThat(entry.getValue()).isInstanceOf(Histogram.class);
            var value = actual.get(entry.getKey());
            assertThat(value).isInstanceOf(Histogram.class);
            assertThat(value.getClass()).isEqualTo(entry.getValue().getClass());
            var reference = (Histogram) entry.getValue();
            var histogram = (Histogram) value;
            assertThat(histogram.getCount()).as(entry.getKey()).isEqualTo(reference.getCount());
            var statistics = histogram.getStatistics();
            assertThat(statistics.size()).isEqualTo(reference.getStatistics().size());
            if (histogram.getCount() > 0) {
                // Flink LatencyStats measures wall-clock milliseconds, not deterministic SQL
                // results. Keep its actual samples and validate their definitions, not equality.
                assertThat(statistics.getMin()).isGreaterThanOrEqualTo(0);
                assertThat(statistics.getMax()).isGreaterThanOrEqualTo(statistics.getMin());
                assertThat(statistics.getMean()).isFinite().isBetween((double) statistics.getMin(), (double)
                        statistics.getMax());
                assertThat(statistics.getStdDev()).isFinite().isGreaterThanOrEqualTo(0);
                for (double quantile : new double[] {0.5, 0.95, 0.99})
                    assertThat(statistics.getQuantile(quantile))
                            .isFinite()
                            .isBetween((double) statistics.getMin(), (double) statistics.getMax());
            }
        }
    }
}
