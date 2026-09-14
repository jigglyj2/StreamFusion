/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativeGaugeDescriptor;
import tech.streamfusion.proto.plan.v1.NativeGaugeSchema;
import tech.streamfusion.proto.plan.v1.NativeGaugeValueKind;

class NativeStageStatisticsTest {
    @Test
    void preservesFlinkViewsCachedLongValuesAndOneSamplePerUnorderedUpdateRound() {
        var first = new InterceptingOperatorMetricGroup();
        var second = new InterceptingOperatorMetricGroup();
        var calls = new AtomicInteger();
        var sample = new AtomicReference<>(new long[] {5, 9, -1});
        var statistics = new NativeStageStatistics(
                schema(
                        descriptor(2, "rocksdb.bytes_written"),
                        descriptor(2, "rocksdb.bytes_read"),
                        descriptor(3, "rocksdb.bytes_written")),
                id -> id == 2 ? first : second,
                () -> {
                    calls.incrementAndGet();
                    return sample.get();
                });
        var a = (Gauge<?>) first.get("rocksdb.bytes_written");
        var b = (Gauge<?>) first.get("rocksdb.bytes_read");
        var c = (Gauge<?>) second.get("rocksdb.bytes_written");
        for (var gauge : List.of(a, b, c)) {
            assertThat(gauge).isInstanceOf(View.class);
            assertThat(gauge.getValue()).isEqualTo(0L);
        }
        assertThat(calls).hasValue(0);
        for (var gauge : List.of(c, a, b)) ((View) gauge).update();
        assertThat(calls).hasValue(1);
        assertThat(a.getValue()).isEqualTo(5L);
        assertThat(b.getValue()).isEqualTo(9L);
        assertThat(c.getValue()).isEqualTo(-1L);
        sample.set(new long[] {7, 10, Long.MIN_VALUE});
        for (int i = 0; i < 20; i++) assertThat(a.getValue()).isEqualTo(5L);
        assertThat(calls).hasValue(1);
        for (var gauge : List.of(b, c, a)) ((View) gauge).update();
        assertThat(calls).hasValue(2);
        assertThat(a.getValue()).isEqualTo(7L);
        assertThat(c.getValue()).isEqualTo(Long.MIN_VALUE);
        statistics.close();
        for (var gauge : List.of(a, b, c)) ((View) gauge).update();
        assertThat(calls).hasValue(2);
        assertThat(a.getValue()).isEqualTo(7L);
        statistics.close();
    }

    @Test
    void rejectsWholeMalformedSchemaBeforeRegistrationAndNeverSamplesEmptyOrClosedViews() {
        var good = descriptor(2, "rocksdb.bytes_written");
        for (var bad : List.of(
                good,
                good.toBuilder().setPlanNodeId(0).build(),
                good.toBuilder().setName("numRecordsIn").build(),
                good.toBuilder().addGroups("column-family").build(),
                good.toBuilder().setValueKindValue(123).build(),
                good.toBuilder().setMetricKindValue(123).build())) {
            var group = new InterceptingOperatorMetricGroup();
            assertThatThrownBy(() -> new NativeStageStatistics(schema(good, bad), id -> group, () -> new long[0]))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(group.get("rocksdb.bytes_written")).isNull();
        }
        var group = new InterceptingOperatorMetricGroup();
        assertThatThrownBy(() -> new NativeStageStatistics(schema(good), id -> null, () -> new long[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NativeStageStatistics(new byte[0], id -> group, () -> new long[0]))
                .hasMessageContaining("version");
        new NativeStageStatistics(schema(), id -> group, () -> {
                    throw new AssertionError("empty sample");
                })
                .close();
        var calls = new AtomicInteger();
        var stats = new NativeStageStatistics(schema(good), id -> group, () -> {
            calls.incrementAndGet();
            return new long[0];
        });
        var gauge = (Gauge<?>) group.get("rocksdb.bytes_written");
        ((View) gauge).update();
        ((View) gauge).update();
        assertThat(gauge.getValue()).isEqualTo(0L);
        stats.close();
        ((View) gauge).update();
        assertThat(calls).hasValue(1);
    }

    @Test
    void closeWaitsForAnActiveSampleAndPreventsSubsequentCalls() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var closed = new java.util.concurrent.CountDownLatch(1);
        var group = new InterceptingOperatorMetricGroup();
        var calls = new AtomicInteger();
        var stats = new NativeStageStatistics(schema(descriptor(2, "rocksdb.bytes_written")), id -> group, () -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS))
                    throw new AssertionError("sample timed out");
            } catch (InterruptedException failure) {
                throw new AssertionError(failure);
            }
            return new long[] {11};
        });
        var view = (View) group.get("rocksdb.bytes_written");
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var updating = executor.submit(view::update);
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var closing = executor.submit(() -> {
                stats.close();
                closed.countDown();
            });
            assertThat(closed.await(50, java.util.concurrent.TimeUnit.MILLISECONDS))
                    .isFalse();
            release.countDown();
            updating.get(10, java.util.concurrent.TimeUnit.SECONDS);
            closing.get(10, java.util.concurrent.TimeUnit.SECONDS);
            view.update();
            assertThat(calls).hasValue(1);
            assertThat(((Gauge<?>) view).getValue()).isEqualTo(11L);
        } finally {
            release.countDown();
            executor.shutdownNow();
            stats.close();
        }
    }

    private static NativeGaugeDescriptor descriptor(long id, String name) {
        return NativeGaugeDescriptor.newBuilder()
                .setPlanNodeId(id)
                .setName(name)
                .setValueKind(NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT64)
                .build();
    }

    private static byte[] schema(NativeGaugeDescriptor... descriptors) {
        return NativeGaugeSchema.newBuilder()
                .setProtocolVersion(1)
                .addAllGauges(List.of(descriptors))
                .build()
                .toByteArray();
    }
}
