/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.*;

class NativeStageTypedMetricsTest {
    @Test
    void publishesRealCounterAndMeterAndReadsFlinkClockWithoutNativeReporterCalls() {
        var group = new InterceptingOperatorMetricGroup();
        var calls = new AtomicInteger();
        var sample = new AtomicReference<>(new long[] {3, -1});
        var time = new AtomicLong(200);
        var metrics = new NativeStageGauges(
                schema(counter(), latency()),
                id -> group,
                () -> {
                    calls.incrementAndGet();
                    return sample.get();
                },
                time::get);
        var counter = (Counter) group.get("dropped");
        var meter = (MeterView) group.get("dropRate");
        var latency = (Gauge<?>) group.get("latency");
        assertThat(counter.getCount()).isEqualTo(3);
        assertThat(meter.getCount()).isEqualTo(3);
        assertThat(latency.getValue()).isEqualTo(0L);
        sample.set(new long[] {9, 100});
        metrics.update();
        assertThat(counter.getCount()).isEqualTo(9);
        assertThat(meter.getCount()).isEqualTo(9);
        assertThat(latency.getValue()).isEqualTo(100L);
        time.set(500);
        for (int read = 0; read < 10; read++) assertThat(latency.getValue()).isEqualTo(400L);
        assertThat(calls).hasValue(2);
        meter.update();
        assertThat(meter.getRate()).isEqualTo(9.0 / 60);
        sample.set(new long[] {11, 1000});
        metrics.update();
        assertThat(latency.getValue()).isEqualTo(-500L);
        sample.set(new long[] {100});
        assertThatThrownBy(metrics::update).hasMessageContaining("shape");
        assertThat(counter.getCount()).isEqualTo(11);
        assertThat(meter.getCount()).isEqualTo(11);
    }

    @Test
    void counterDeltasRetainFlinkLongOverflowSemantics() {
        var group = new InterceptingOperatorMetricGroup();
        var sample = new AtomicReference<>(new long[] {Long.MAX_VALUE});
        var metrics = new NativeStageGauges(schema(counter()), id -> group, sample::get);
        sample.set(new long[] {Long.MIN_VALUE});
        metrics.update();
        assertThat(((Counter) group.get("dropped")).getCount()).isEqualTo(Long.MIN_VALUE);
        assertThat(((MeterView) group.get("dropRate")).getCount()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void rejectsIncompatibleTypesVersionsClockAndMeterCollisionsBeforeRegistration() {
        var group = new InterceptingOperatorMetricGroup();
        var counter = counter();
        var aliasCollision =
                counter.toBuilder().setName("dropRate").setMeterName("").build();
        for (byte[] schema : List.of(
                NativeGaugeSchema.newBuilder()
                        .setProtocolVersion(1)
                        .addGauges(counter)
                        .build()
                        .toByteArray(),
                schema(counter.toBuilder()
                        .setValueKind(NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_FLOAT64)
                        .build()),
                schema(counter.toBuilder().setMetricKindValue(999).build()),
                schema(counter, aliasCollision),
                schema(counter.toBuilder().setMeterName("numRecordsIn").build()),
                schema(latency().toBuilder().setMeterName("dropRate").build()),
                schema(latency()))) {
            assertThatThrownBy(() -> new NativeStageGauges(schema, id -> group, () -> {
                        throw new AssertionError("Invalid schema must not sample native values");
                    }))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(group.get("dropped")).isNull();
            assertThat(group.get("dropRate")).isNull();
            assertThat(group.get("latency")).isNull();
        }
    }

    private static NativeGaugeDescriptor counter() {
        return NativeGaugeDescriptor.newBuilder()
                .setPlanNodeId(3)
                .setName("dropped")
                .setMeterName("dropRate")
                .setValueKind(NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT64)
                .setMetricKind(NativeMetricKind.NATIVE_METRIC_KIND_COUNTER)
                .build();
    }

    private static NativeGaugeDescriptor latency() {
        return NativeGaugeDescriptor.newBuilder()
                .setPlanNodeId(3)
                .setName("latency")
                .setValueKind(NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT64)
                .setMetricKind(NativeMetricKind.NATIVE_METRIC_KIND_WATERMARK_LATENCY)
                .build();
    }

    private static byte[] schema(NativeGaugeDescriptor... descriptors) {
        return NativeGaugeSchema.newBuilder()
                .setProtocolVersion(2)
                .addAllGauges(List.of(descriptors))
                .build()
                .toByteArray();
    }
}
