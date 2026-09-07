/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.compare;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.metrics;
import static tech.streamfusion.flink.planner.SharedAggregateMetricSurfaceTest.stageGroup;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Fail both engines after the same emitted logical records, at a native output-batch boundary. */
class SharedMiniBatchFailureMetricTest {
    @Test
    void partialControlOutputAndCompleteDefaultMetricsMatchFlinkOnConsumerFailure() throws Exception {
        for (boolean rocks : List.of(false, true))
            try (var oracle = SharedAggregateFlinkOracle.create(rocks, 100_000);
                    var target = new SharedAggregateRuntimeHarness(
                            rocks, SharedMiniBatchControlTest.plan(100_000), 128L << 20);
                    var allocator = new RootAllocator(64L << 20)) {
                target.failed = true;
                var inputWatermark = new WatermarkGauge();
                var outputWatermark = new WatermarkGauge();
                var flinkMetrics = oracle.getOperator().getMetricGroup();
                flinkMetrics.gauge("currentInputWatermark", inputWatermark);
                flinkMetrics.gauge("currentOutputWatermark", outputWatermark);
                var failure = new IllegalStateException("test sink failed after the first bounded output");
                var emitted = new AtomicInteger();
                var field = AbstractStreamOperator.class.getDeclaredField("output");
                field.setAccessible(true);
                Object original = field.get(oracle.getOperator());
                field.set(
                        oracle.getOperator(),
                        Proxy.newProxyInstance(
                                Output.class.getClassLoader(),
                                new Class<?>[] {Output.class},
                                (proxy, method, arguments) -> {
                                    Object result;
                                    try {
                                        result = method.invoke(original, arguments);
                                    } catch (InvocationTargetException error) {
                                        throw error.getCause();
                                    }
                                    if (method.getName().equals("collect") && arguments.length == 1) {
                                        flinkMetrics
                                                .getIOMetricGroup()
                                                .getNumRecordsOutCounter()
                                                .inc();
                                        if (emitted.incrementAndGet() == 2048) throw failure;
                                    }
                                    return result;
                                }));
                // Flink's bundle collector captures the output during open; point that collector
                // at the fault-injecting output as well, without replacing its execution algorithm.
                var bundleClass = org.apache.flink.table.runtime.operators.bundle.AbstractMapBundleOperator.class;
                var collector = bundleClass.getDeclaredField("collector");
                collector.setAccessible(true);
                collector.set(
                        oracle.getOperator(), new org.apache.flink.table.runtime.util.StreamRecordCollector<>((Output)
                                field.get(oracle.getOperator())));
                var rows = new ArrayList<GenericRowData>();
                for (int i = 0; i < 2500; i++) {
                    var row = GenericRowData.of(StringData.fromString("é-" + i), (long) i);
                    rows.add(row);
                    flinkMetrics.getIOMetricGroup().getNumRecordsInCounter().inc();
                    oracle.processElement(new StreamRecord<>(row));
                }
                try (var batch = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)) {
                    target.processElement(0, new StreamRecord<>(batch));
                }
                compare(metrics(flinkMetrics), metrics(stageGroup(target, 3)));
                target.afterOutput = count -> {
                    assertThat(count).isEqualTo(2048);
                    throw failure;
                };
                inputWatermark.setCurrentWatermark(100);
                assertThatThrownBy(() -> oracle.processWatermark(new Watermark(100)))
                        .isSameAs(failure);
                var actualFailure = org.assertj.core.api.Assertions.catchThrowable(
                        () -> target.processWatermark(0, new Watermark(100)));
                if (actualFailure != failure)
                    throw new AssertionError("Native flush failed before the injected sink failure", actualFailure);
                assertThat(emitted).hasValue(2048);
                assertThat(target.eventOrder).noneMatch(event -> event.startsWith("watermark:"));
                var expected = new DataOutputSerializer(128);
                var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
                for (var record : oracle.extractOutputStreamRecords()) {
                    assertThat(record.hasTimestamp()).isFalse();
                    serializer.serialize(record.getValue(), expected);
                }
                assertThat(target.captured.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                compare(metrics(flinkMetrics), metrics(stageGroup(target, 3)));
                assertThatThrownBy(() -> target.region().prepareSnapshotPreBarrier(7))
                        .hasMessageContaining("recovery");
            }
    }
}
