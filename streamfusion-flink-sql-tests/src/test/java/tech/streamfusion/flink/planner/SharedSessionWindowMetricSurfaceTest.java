/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.SharedSessionWindowFixture.*;

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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** SQL-generated SESSION oracle versus the shared native metric and control lifecycle. */
class SharedSessionWindowMetricSurfaceTest {

    @Test
    void arrivalOrderMergingMatchesChangelogAndCompleteMetricSurface() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int seed : List.of(3, 19, 71))
                for (int batchSize : List.of(1, 7, 31))
                    try (var comparison = new Comparison(rocks)) {
                        var random = new Random(seed);
                        comparison.compare();
                        comparison.input(List.of(row(10000)));
                        comparison.watermark(15000);
                        comparison.input(List.of(row(0)));
                        comparison.watermark(19999);
                        for (int phase = 0; phase < 8; phase++) {
                            long base = 40000 + phase * 40000L;
                            comparison.watermark(base + 15000);
                            var rows = new ArrayList<RowData>();
                            rows.add(row(base));
                            rows.add(row(base + 10000));
                            rows.add(row(base));
                            for (int i = 0; i < 61; i++) rows.add(row(base + (random.nextInt(51) - 10) * 1000L));
                            for (int offset = 0; offset < rows.size(); offset += batchSize)
                                comparison.input(rows.subList(offset, Math.min(rows.size(), offset + batchSize)));
                            comparison.watermark(base + 39999);
                            comparison.time(phase * 1234L);
                            comparison.time(phase * 1234L + 100);
                            comparison.flink.prepareSnapshotPreBarrier(phase);
                            comparison.target.region().prepareSnapshotPreBarrier(phase);
                            comparison.compare();
                        }
                        comparison.watermark(Long.MAX_VALUE);
                        comparison.target.region().endInput(1);
                        comparison.target.region().finish();
                        comparison.flink.getOperator().finish();
                        comparison.compare();
                    }
    }

    private static final class Comparison implements AutoCloseable {
        final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink;
        final KeyedNativeMetricHarness target;
        final RootAllocator allocator = new RootAllocator(64L << 20);
        final WatermarkGauge input = new WatermarkGauge();
        final WatermarkGauge output = new WatermarkGauge();

        Comparison(boolean rocks) throws Exception {
            flink = GlobalWindowFlinkOracle.create(SlicingWindowFlinkPlan.stage("WindowAggregate", SQL), rocks, null);
            target = new KeyedNativeMetricHarness(rocks, plan(), List.of(INPUT), OUTPUT, List.of(3L));
            flink.getOperator().getMetricGroup().gauge("currentInputWatermark", input);
            flink.getOperator().getMetricGroup().gauge("currentOutputWatermark", output);
        }

        org.apache.flink.table.types.logical.RowType outputType() {
            return OUTPUT;
        }

        void input(List<RowData> rows) throws Exception {
            var serializer = new RowDataSerializer(INPUT);
            for (var row : rows) {
                flink.getOperator()
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsInCounter()
                        .inc();
                flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row), 123));
            }
            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                target.processElement(0, new StreamRecord<>(batch));
            }
            compare();
        }

        void watermark(long value) throws Exception {
            input.setCurrentWatermark(value);
            flink.processWatermark(new Watermark(value));
            target.processWatermark(0, new Watermark(value));
            compare();
        }

        void time(long value) throws Exception {
            flink.setProcessingTime(value);
            target.setProcessingTime(value);
            compare();
        }

        void compare() throws Exception {
            var bytes = new DataOutputSerializer(128);
            for (var event : flink.getOutput()) {
                if (event instanceof StreamRecord<?>)
                    flink.getOperator()
                            .getMetricGroup()
                            .getIOMetricGroup()
                            .getNumRecordsOutCounter()
                            .inc();
                if (event instanceof Watermark) output.setCurrentWatermark(((Watermark) event).getTimestamp());
                StageEventBytes.encode(outputType(), (StreamElement) event, bytes);
            }
            flink.getOutput().clear();
            target.drainControls();
            assertThat(target.output.getCopyOfBuffer()).containsExactly(bytes.getCopyOfBuffer());
            target.output.clear();
            var expected = RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup());
            var actual = RegisteredMetricSurface.metrics(target.stage(3));
            var expectedRate = (MeterView) expected.get("lateRecordsDroppedRate");
            var actualRate = (MeterView) actual.get("lateRecordsDroppedRate");
            expectedRate.update();
            actualRate.update();
            assertThat(actualRate.getRate()).isEqualTo(expectedRate.getRate());
            RegisteredMetricSurface.compare(expected, actual);
        }

        public void close() throws Exception {
            try {
                target.close();
            } finally {
                try {
                    flink.close();
                } finally {
                    allocator.close();
                }
            }
        }
    }
}
