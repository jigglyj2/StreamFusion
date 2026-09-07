/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativeGaugeDescriptor;
import tech.streamfusion.proto.plan.v1.NativeGaugeSchema;
import tech.streamfusion.proto.plan.v1.NativeGaugeValueKind;

class NativeStageGaugesTest {
    @Test
    void preservesScalarTypesAndSamplesOncePerUpdateNotPerReporterRead() {
        var group = new InterceptingOperatorMetricGroup();
        var calls = new AtomicInteger();
        var sample = new AtomicReference<>(new long[] {-7, Long.MAX_VALUE, Double.doubleToRawLongBits(-0.0)});
        var schema = schema(
                descriptor("size", NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT32),
                descriptor("bytes", NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT64),
                descriptor("ratio", NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_FLOAT64));
        var gauges = new NativeStageGauges(schema, id -> id == 42 ? group : null, () -> {
            calls.incrementAndGet();
            return sample.get();
        });
        for (int i = 0; i < 10; i++) {
            assertThat(((Gauge<?>) group.get("size")).getValue()).isEqualTo(Integer.valueOf(-7));
            assertThat(((Gauge<?>) group.get("bytes")).getValue()).isEqualTo(Long.MAX_VALUE);
            assertThat(((Gauge<?>) group.get("ratio")).getValue()).isEqualTo(-0.0);
        }
        assertThat(calls).hasValue(1);
        sample.set(new long[] {3, 9, Double.doubleToRawLongBits(1.5)});
        gauges.update();
        assertThat(calls).hasValue(2);
        assertThat(((Gauge<?>) group.get("size")).getValue()).isEqualTo(Integer.valueOf(3));
        assertThat(((Gauge<?>) group.get("ratio")).getValue()).isEqualTo(1.5);
        sample.set(new long[] {1L << 32, 9, 0});
        assertThatThrownBy(gauges::update).hasMessageContaining("sign extended");
        assertThat(((Gauge<?>) group.get("size")).getValue()).isEqualTo(Integer.valueOf(3));
        sample.set(new long[0]);
        assertThatThrownBy(gauges::update).hasMessageContaining("shape");
    }

    @Test
    void validatesWholeSchemaBeforeRegisteringAndSkipsEmptySnapshots() {
        var group = new InterceptingOperatorMetricGroup();
        var good = descriptor("size", NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT32);
        for (var bad : java.util.List.of(
                good.toBuilder().setPlanNodeId(0).build(),
                good.toBuilder().setPlanNodeId(7).build(),
                good.toBuilder().setValueKindValue(123).build(),
                good.toBuilder().setName("").build(),
                good.toBuilder().addGroups("bad/path").build(),
                good.toBuilder().setName("numRecordsIn").build(),
                good)) {
            assertThatThrownBy(() -> new NativeStageGauges(schema(good, bad), id -> id == 42 ? group : null, () -> {
                        throw new AssertionError("Do not sample invalid schemas");
                    }))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(group.get("size")).isNull();
        }
        assertThatThrownBy(() -> new NativeStageGauges(new byte[0], id -> group, () -> new long[0]))
                .hasMessageContaining("version");
        var empty = new NativeStageGauges(schema(), id -> group, () -> {
            throw new AssertionError("Do not call JNI for empty gauge schemas");
        });
        empty.update();
    }

    private static NativeGaugeDescriptor descriptor(String name, NativeGaugeValueKind kind) {
        return NativeGaugeDescriptor.newBuilder()
                .setPlanNodeId(42)
                .setName(name)
                .setValueKind(kind)
                .build();
    }

    private static byte[] schema(NativeGaugeDescriptor... gauges) {
        return NativeGaugeSchema.newBuilder()
                .setProtocolVersion(1)
                .addAllGauges(java.util.List.of(gauges))
                .build()
                .toByteArray();
    }
}
