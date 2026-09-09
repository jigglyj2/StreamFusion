/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Full registered Flink metric and byte/changelog conformance for distinct global TUMBLE. */
class DistinctWindowMetricSurfaceTest {
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void generatedCompositeKeysAndDuplicatePartialsMatchAllMetrics(boolean strings, boolean rocks) throws Exception {
        var fixture = new DistinctWindowFixture(strings);
        for (int seed = 0; seed < 3; seed++)
            for (int batchSize : List.of(7, 31)) {
                try (var flink = fixture.oracle(rocks, null);
                        var target = new KeyedNativeMetricHarness(
                                rocks, fixture.plan(), List.of(fixture.input), fixture.output, List.of(3L));
                        var allocator = new RootAllocator(64L << 20)) {
                    var inputWatermark = new WatermarkGauge();
                    var outputWatermark = new WatermarkGauge();
                    flink.getOperator().getMetricGroup().gauge("currentInputWatermark", inputWatermark);
                    flink.getOperator().getMetricGroup().gauge("currentOutputWatermark", outputWatermark);
                    var serializer = new RowDataSerializer(fixture.flinkInput);
                    var random = new Random(seed);
                    for (int phase = 0; phase < 8; phase++) {
                        for (int start = 0; start < 31; start += batchSize) {
                            var rows = new ArrayList<RowData>();
                            for (int row = start; row < Math.min(31, start + batchSize); row++) {
                                var id = random.nextInt(64);
                                var end = (phase + random.nextInt(9) - 4) * 2000L;
                                flink.getOperator()
                                        .getMetricGroup()
                                        .getIOMetricGroup()
                                        .getNumRecordsInCounter()
                                        .inc();
                                flink.processElement(
                                        new StreamRecord<>(serializer.toBinaryRow(fixture.row(id, end, false)), 123));
                                rows.add(fixture.row(id, end, true));
                            }
                            try (var batch = ArrowRowDataBatch.transpose(rows, fixture.input, allocator)) {
                                target.processElement(0, new StreamRecord<>(batch));
                            }
                            compare(fixture, flink, target, outputWatermark);
                        }
                        var mark = (phase - 1) * 2000L - 1;
                        inputWatermark.setCurrentWatermark(mark);
                        flink.processWatermark(new Watermark(mark));
                        target.processWatermark(0, new Watermark(mark));
                        for (long time : new long[] {phase * 1234L, phase * 1234L + 100}) {
                            flink.setProcessingTime(time);
                            target.setProcessingTime(time);
                            compare(fixture, flink, target, outputWatermark);
                        }
                        flink.prepareSnapshotPreBarrier(phase);
                        target.region().prepareSnapshotPreBarrier(phase);
                        compare(fixture, flink, target, outputWatermark);
                    }
                    inputWatermark.setCurrentWatermark(Long.MAX_VALUE);
                    flink.processWatermark(new Watermark(Long.MAX_VALUE));
                    target.processWatermark(0, new Watermark(Long.MAX_VALUE));
                    target.region().endInput(1);
                    target.region().finish();
                    flink.getOperator().finish();
                    compare(fixture, flink, target, outputWatermark);
                }
            }
    }

    private static void compare(
            DistinctWindowFixture fixture,
            org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink,
            KeyedNativeMetricHarness target,
            WatermarkGauge outputWatermark)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        for (var event : flink.getOutput()) {
            if (event instanceof StreamRecord<?>)
                flink.getOperator()
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsOutCounter()
                        .inc();
            if (event instanceof Watermark) outputWatermark.setCurrentWatermark(((Watermark) event).getTimestamp());
            StageEventBytes.encode(fixture.output, (StreamElement) event, expected);
        }
        flink.getOutput().clear();
        target.drainControls();
        assertThat(WindowTimerEventBytes.canonical(fixture.output, fixture.keys + 1, target.output.getCopyOfBuffer()))
                .containsExactly(
                        WindowTimerEventBytes.canonical(fixture.output, fixture.keys + 1, expected.getCopyOfBuffer()));
        target.output.clear();
        var reference = RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup());
        var actual = RegisteredMetricSurface.metrics(target.stage(3));
        var referenceRate = (MeterView) reference.get("lateRecordsDroppedRate");
        var actualRate = (MeterView) actual.get("lateRecordsDroppedRate");
        referenceRate.update();
        actualRate.update();
        assertThat(actualRate.getRate()).isEqualTo(referenceRate.getRate());
        RegisteredMetricSurface.compare(reference, actual);
    }
}
