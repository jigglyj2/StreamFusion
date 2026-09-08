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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Compare the complete registered Flink global-window surface, including real counter/meter types. */
class SharedWindowMetricSurfaceTest {
    protected boolean tumbling() {
        return false;
    }

    @Test
    void generatedPartialsMatchAllMetricsChangelogsAndLiveClockSemantics() throws Exception {
        for (boolean attached : List.of(false, true))
            for (boolean rocks : List.of(false, true))
                for (int seed = 0; seed < 3; seed++)
                    for (int batchSize : List.of(7, 31))
                        try (var comparison = new Comparison(attached, rocks, tumbling())) {
                            var random = new Random(seed);
                            comparison.compare();
                            for (int phase = 0; phase < 8; phase++) {
                                var rows = new ArrayList<RowData>();
                                for (int row = 0; row < 31; row++)
                                    rows.add(GenericRowData.of(
                                            1L,
                                            (long) random.nextInt(99) + 1,
                                            1L,
                                            (phase + random.nextInt(9) - 4) * 2000L));
                                for (int offset = 0; offset < rows.size(); offset += batchSize)
                                    comparison.input(rows.subList(offset, Math.min(rows.size(), offset + batchSize)));
                                comparison.watermark((phase - 1) * 2000L - 1);
                                // No input/native metric update between these reads. The gauge must
                                // use Flink's current processing clock, even when latency is negative.
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
        final boolean attached;
        final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink;
        final KeyedNativeMetricHarness target;
        final RootAllocator allocator = new RootAllocator(64L << 20);
        final WatermarkGauge input = new WatermarkGauge();
        final WatermarkGauge output = new WatermarkGauge();

        Comparison(boolean attached, boolean rocks, boolean tumble) throws Exception {
            this.attached = attached;
            flink = attached
                    ? GlobalWindowFlinkOracle.create(
                            SlicingWindowFlinkPlan.stage(
                                    "GlobalWindowAggregate", AttachedSlicingWindowFixture.sql(true)),
                            rocks,
                            null)
                    : GlobalWindowFlinkOracle.create(rocks, null, tumble);
            target = new KeyedNativeMetricHarness(
                    rocks,
                    attached ? AttachedSlicingWindowFixture.plan(true) : SharedSlicingWindowFixture.plan(tumble),
                    List.of(SharedSlicingWindowFixture.INPUT),
                    outputType(),
                    List.of(3L));
            // The bare harness omits task-owned input/output wrappers and watermark gauges.
            // Use Flink's real definitions; count only rows actually accepted/emitted by Flink.
            flink.getOperator().getMetricGroup().gauge("currentInputWatermark", input);
            flink.getOperator().getMetricGroup().gauge("currentOutputWatermark", output);
        }

        org.apache.flink.table.types.logical.RowType outputType() {
            return attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
        }

        void input(List<RowData> rows) throws Exception {
            var nativeRows = new ArrayList<RowData>();
            var serializer = new RowDataSerializer(
                    attached ? AttachedSlicingWindowFixture.FLINK_INPUT : SharedSlicingWindowFixture.FLINK_INPUT);
            for (var row : rows) {
                var flinkRow = attached ? row : GenericRowData.of(row.getLong(0), row.getLong(1), row.getLong(3));
                flink.getOperator()
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsInCounter()
                        .inc();
                flink.processElement(new StreamRecord<>(serializer.toBinaryRow(flinkRow), 123));
                nativeRows.add(GenericRowData.of(
                        row.getLong(0),
                        attached
                                ? AttachedSlicingWindowFixture.partial(row.getLong(1), row.getLong(2))
                                : SharedSlicingWindowFixture.count(row.getLong(1)),
                        row.getLong(3) - (attached ? 6000 : 2000),
                        row.getLong(3)));
            }
            try (var batch = ArrowRowDataBatch.transpose(nativeRows, SharedSlicingWindowFixture.INPUT, allocator)) {
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
