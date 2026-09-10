/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Generated SQL-handler parity for string extrema, filtered counts and composite keys. */
class StringAggregateRuntimeParityTest {
    @ParameterizedTest
    @CsvSource({"false", "true"})
    void generatedStringExtremaMatchFlinkChangelogControlsAndMetrics(boolean rocks) throws Exception {
        try (var oracle = StringAggregateFlinkOracle.create(rocks, null, true);
                var nativePlan = new KeyedNativeMetricHarness(
                        rocks,
                        StringAggregateFixture.plan(),
                        List.of(StringAggregateFlinkOracle.INPUT),
                        StringAggregateFlinkOracle.OUTPUT,
                        List.of(3L));
                var allocator = new RootAllocator(64L << 20)) {
            var expected = new DataOutputSerializer(128);
            var group = oracle.getOperator().getMetricGroup();
            var nativeGroup = SharedAggregateMetricSurfaceTest.stageGroup(nativePlan.region(), 3);
            var inputWatermark = new org.apache.flink.streaming.runtime.metrics.WatermarkGauge();
            var outputWatermark = new org.apache.flink.streaming.runtime.metrics.WatermarkGauge();
            group.gauge("currentInputWatermark", inputWatermark);
            group.gauge("currentOutputWatermark", outputWatermark);
            long timestamp = 0;
            for (int seed : new int[] {3, 29, 197}) {
                for (int pass = 0; pass < 3; pass++) {
                    var inputs = StringAggregateRecoveryFixture.rows(seed, pass);
                    for (int start = 0; start < inputs.size(); start += 31) {
                        var rows = new ArrayList<GenericRowData>();
                        int count = Math.min(31, inputs.size() - start);
                        var kinds = new RowKind[count];
                        var present = new boolean[count];
                        var times = new long[count];
                        for (int index = 0; index < count; index++) {
                            var value = inputs.get(start + index);
                            value.setRowKind(RowKind.INSERT);
                            rows.add(value);
                            kinds[index] = value.getRowKind();
                            present[index] = index % 3 != 0;
                            times[index] = timestamp++;
                            group.getIOMetricGroup().getNumRecordsInCounter().inc();
                            var transported = StringAggregateFlinkOracle.binaryInput(value);
                            oracle.processElement(
                                    present[index]
                                            ? new StreamRecord<>(transported, times[index])
                                            : new StreamRecord<>(transported));
                        }
                        group.getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .inc(oracle.extractOutputStreamRecords().size());
                        for (var event : oracle.getOutput())
                            StageEventBytes.encode(
                                    StringAggregateFlinkOracle.OUTPUT,
                                    (org.apache.flink.streaming.runtime.streamrecord.StreamElement) event,
                                    expected);
                        oracle.getOutput().clear();
                        try (var batch = ArrowRowDataBatch.transpose(rows, StringAggregateFlinkOracle.INPUT, allocator)
                                .withEnvelope(kinds, present, times)) {
                            nativePlan.processElement(0, new StreamRecord<>(batch));
                        }
                        var actualBytes = nativePlan.output.getCopyOfBuffer();
                        var expectedBytes = expected.getCopyOfBuffer();
                        if (!java.util.Arrays.equals(actualBytes, expectedBytes)) {
                            java.nio.file.Files.write(
                                    java.nio.file.Path.of("target/string-aggregate-actual.bin"), actualBytes);
                            java.nio.file.Files.write(
                                    java.nio.file.Path.of("target/string-aggregate-expected.bin"), expectedBytes);
                        }
                        assertThat(java.util.Arrays.equals(actualBytes, expectedBytes))
                                .withFailMessage(
                                        "rocks=%s seed=%s pass=%s start=%s mismatch=%s lengths=%s/%s",
                                        rocks,
                                        seed,
                                        pass,
                                        start,
                                        java.util.Arrays.mismatch(actualBytes, expectedBytes),
                                        actualBytes.length,
                                        expectedBytes.length)
                                .isTrue();
                        nativePlan.output.clear();
                        expected.clear();
                        inputWatermark.setCurrentWatermark(timestamp);
                        oracle.processWatermark(new Watermark(timestamp));
                        for (var event : oracle.getOutput()) {
                            if (event instanceof Watermark)
                                outputWatermark.setCurrentWatermark(((Watermark) event).getTimestamp());
                            StageEventBytes.encode(
                                    StringAggregateFlinkOracle.OUTPUT,
                                    (org.apache.flink.streaming.runtime.streamrecord.StreamElement) event,
                                    expected);
                        }
                        oracle.getOutput().clear();
                        nativePlan.processWatermark(0, new Watermark(timestamp));
                        var actualControls = new DataOutputSerializer(128);
                        for (var event : nativePlan.controls)
                            StageEventBytes.encode(StringAggregateFlinkOracle.OUTPUT, event, actualControls);
                        assertThat(actualControls.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                        nativePlan.controls.clear();
                        expected.clear();
                        RegisteredMetricSurface.compare(
                                RegisteredMetricSurface.metrics(group), RegisteredMetricSurface.metrics(nativeGroup));
                    }
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
