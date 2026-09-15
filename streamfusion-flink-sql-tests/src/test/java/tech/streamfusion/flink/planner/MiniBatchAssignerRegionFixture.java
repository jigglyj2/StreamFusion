/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.wmassigners.ProcTimeMiniBatchAssignerOperator;
import org.apache.flink.table.runtime.operators.wmassigners.RowTimeMiniBatchAssginerOperator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.minibatch.MiniBatchAssignerTestAccess;

/** Real assigners directly composed with SQL-planned Flink and common native aggregate regions. */
final class MiniBatchAssignerRegionFixture implements AutoCloseable {
    final SharedAggregateRuntimeHarness nativeRegion;
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flinkRegion;
    final OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> nativeAssigner;
    final OneInputStreamOperatorTestHarness<RowData, RowData> flinkAssigner;
    final ScriptedClock nativeClock = new ScriptedClock();
    final ScriptedClock flinkClock = new ScriptedClock();
    final DataOutputSerializer nativeEvents = new DataOutputSerializer(128);
    final DataOutputSerializer flinkEvents = new DataOutputSerializer(128);
    final WatermarkGauge regionInputWatermark = new WatermarkGauge();
    final WatermarkGauge regionOutputWatermark = new WatermarkGauge();

    @SuppressWarnings("unchecked")
    MiniBatchAssignerRegionFixture(boolean rocks, boolean processingTime, long interval) throws Exception {
        nativeRegion = new SharedAggregateRuntimeHarness(rocks, SharedMiniBatchControlTest.plan(7), 16L << 20);
        flinkRegion = SharedAggregateFlinkOracle.create(rocks, 7);
        flinkRegion.getOperator().getMetricGroup().gauge("currentInputWatermark", regionInputWatermark);
        flinkRegion.getOperator().getMetricGroup().gauge("currentOutputWatermark", regionOutputWatermark);
        AbstractStreamOperator<RowData> original = processingTime
                ? new ProcTimeMiniBatchAssignerOperator(interval)
                : new RowTimeMiniBatchAssginerOperator(interval);
        var accelerated = MiniBatchAssignerTestAccess.create(processingTime, interval);
        flinkAssigner = new OneInputStreamOperatorTestHarness<>((OneInputStreamOperator<RowData, RowData>) original);
        nativeAssigner = new OneInputStreamOperatorTestHarness<>(
                (OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>) accelerated);
        flinkAssigner.setOutputCreator(ignored -> new CollectorOutput<RowData>(new ArrayList<>()) {
            @Override
            public void collect(StreamRecord<RowData> record) {
                original.getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsOutCounter()
                        .inc();
                writeRecord(flinkEvents, record.getValue(), record.hasTimestamp(), record.getTimestamp());
                try {
                    flinkRegion
                            .getOperator()
                            .getMetricGroup()
                            .getIOMetricGroup()
                            .getNumRecordsInCounter()
                            .inc();
                    flinkRegion.processElement(record);
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                try {
                    writeWatermark(flinkEvents, watermark);
                    regionInputWatermark.setCurrentWatermark(watermark.getTimestamp());
                    flinkRegion.processWatermark(watermark);
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            }
        });
        nativeAssigner.setOutputCreator(ignored -> new CollectorOutput<ArrowRowDataBatch>(new ArrayList<>()) {
            @Override
            public void collect(StreamRecord<ArrowRowDataBatch> record) {
                accelerated
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .getNumRecordsOutCounter()
                        .inc();
                var batch = record.getValue();
                for (int row = 0; row < batch.size(); row++) {
                    var view = batch.rowView(row);
                    view.setRowKind(batch.rowKind(row));
                    writeRecord(nativeEvents, view, batch.hasTimestamp(row), batch.timestamp(row));
                }
                try {
                    nativeRegion.processElement(0, record);
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                try {
                    writeWatermark(nativeEvents, watermark);
                    nativeRegion.processWatermark(0, watermark);
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            }
        });
        flinkAssigner.setup(new RowDataSerializer(SharedAggregateFlinkOracle.INPUT));
        nativeAssigner.setup(ArrowRowDataBatchSerializer.INSTANCE);
        StreamOperatorUtils.setProcessingTimeService(original, flinkClock);
        StreamOperatorUtils.setProcessingTimeService(accelerated, nativeClock);
        flinkAssigner.open();
        nativeAssigner.open();
    }

    void input(ArrowRowDataBatch batch, List<Long> samples) throws Exception {
        flinkClock.samples.addAll(samples);
        nativeClock.samples.addAll(samples);
        for (int row = 0; row < batch.size(); row++) {
            var view = batch.rowView(row);
            view.setRowKind(batch.rowKind(row));
            flinkAssigner
                    .getOperator()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .getNumRecordsInCounter()
                    .inc();
            flinkAssigner.processElement(
                    batch.hasTimestamp(row)
                            ? new StreamRecord<>(view, batch.timestamp(row))
                            : new StreamRecord<>(view));
        }
        nativeAssigner
                .getOperator()
                .getMetricGroup()
                .getIOMetricGroup()
                .getNumRecordsInCounter()
                .inc();
        nativeAssigner.processElement(new StreamRecord<>(batch));
        assertThat(nativeClock.samples)
                .as("one Flink clock sample per logical record")
                .containsExactlyElementsOf(flinkClock.samples);
        check();
    }

    void watermark(long timestamp) throws Exception {
        flinkAssigner.processWatermark(new Watermark(timestamp));
        nativeAssigner.processWatermark(new Watermark(timestamp));
        check();
    }

    void check() throws Exception {
        assertThat(nativeEvents.getCopyOfBuffer())
                .as("assigner record bytes, envelopes and watermark ordering")
                .isEqualTo(flinkEvents.getCopyOfBuffer());
        RegisteredMetricSurface.compare(
                RegisteredMetricSurface.metrics(flinkAssigner.getOperator().getMetricGroup()),
                RegisteredMetricSurface.metrics(nativeAssigner.getOperator().getMetricGroup()));
        SharedMiniBatchMetricSurfaceTest.check(nativeRegion, flinkRegion, regionOutputWatermark);
        nativeEvents.clear();
        flinkEvents.clear();
    }

    void finish() throws Exception {
        flinkAssigner.getOperator().finish();
        nativeAssigner.getOperator().finish();
        flinkRegion.getOperator().finish();
        nativeRegion.region().endInput(1);
        nativeRegion.region().finish();
        check();
    }

    private static void writeRecord(DataOutputSerializer output, RowData row, boolean present, long timestamp) {
        try {
            output.writeByte(0);
            output.writeBoolean(present);
            if (present) output.writeLong(timestamp);
            new RowDataSerializer(SharedAggregateFlinkOracle.INPUT).serialize(row, output);
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static void writeWatermark(DataOutputSerializer output, Watermark watermark) throws java.io.IOException {
        output.writeByte(1);
        output.writeLong(watermark.getTimestamp());
    }

    @Override
    public void close() throws Exception {
        try (var a = nativeRegion;
                var b = flinkRegion;
                var c = nativeAssigner;
                var d = flinkAssigner) {}
    }

    static final class ScriptedClock extends TestProcessingTimeService {
        final ArrayDeque<Long> samples = new ArrayDeque<>();
        long now;

        @Override
        public long getCurrentProcessingTime() {
            if (!samples.isEmpty()) now = samples.removeFirst();
            return now;
        }
    }
}
