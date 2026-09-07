/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

class RowDataToArrowBatchOperatorTest {
    private static final RowType TYPE = RowType.of(new IntType());

    @Test
    void quietSourceFlushesGeneratedChangelogAtConfiguredDeadline() throws Exception {
        Capture capture = new Capture();
        try (var harness = harness(-1, capture)) {
            harness.getEnvironment().getJobConfiguration().set(ExecutionOptions.BUFFER_TIMEOUT, Duration.ofMillis(25));
            harness.open();
            Random random = new Random(815);
            DataOutputSerializer expected = new DataOutputSerializer(256);
            RowDataSerializer serializer = new RowDataSerializer(TYPE);
            for (int index = 0; index < 73; index++) {
                GenericRowData row = GenericRowData.of(random.nextInt());
                row.setRowKind(RowKind.values()[index % 4]);
                serializer.serialize(row, expected);
                harness.processElement(new StreamRecord<>(row, index));
            }
            harness.setProcessingTime(24);
            assertThat(capture.events).isEmpty();
            harness.setProcessingTime(25);
            assertThat(capture.bytes.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
            assertThat(capture.events).containsExactly("batch:73");
            assertThat(capture.timestamps)
                    .containsExactlyElementsOf(java.util.stream.LongStream.range(0, 73)
                            .boxed()
                            .collect(java.util.stream.Collectors.toList()));
            harness.setProcessingTime(1000);
            assertThat(capture.events).hasSize(1);
        }
    }

    @Test
    void controlFlushCancelsOldDeadlineAndNextBatchGetsItsOwnDeadline() throws Exception {
        Capture capture = new Capture();
        try (var harness = harness(20, capture)) {
            harness.open();
            harness.processElement(new StreamRecord<>(GenericRowData.of(1)));
            harness.setProcessingTime(5);
            harness.processWatermark(new Watermark(7));
            harness.setProcessingTime(10);
            harness.processElement(new StreamRecord<>(GenericRowData.of(2)));
            harness.setProcessingTime(20);
            assertThat(capture.events).containsExactly("batch:1", "watermark:7");
            harness.setProcessingTime(30);
            assertThat(capture.events).containsExactly("batch:1", "watermark:7", "batch:1");
        }
    }

    @Test
    void zeroTimeoutFlushesEachRecordAndDisabledTimeoutWaitsForControl() throws Exception {
        Capture immediate = new Capture();
        try (var harness = harness(0, immediate)) {
            harness.open();
            harness.processElement(new StreamRecord<>(GenericRowData.of(1)));
            assertThat(immediate.events).containsExactly("batch:1");
        }
        Capture disabled = new Capture();
        try (var harness = harness(-1, disabled)) {
            harness.getEnvironment().getJobConfiguration().set(ExecutionOptions.BUFFER_TIMEOUT_ENABLED, false);
            harness.open();
            harness.processElement(new StreamRecord<>(GenericRowData.of(1)));
            harness.setProcessingTime(1000);
            assertThat(disabled.events).isEmpty();
            harness.getOperator().prepareSnapshotPreBarrier(1);
            assertThat(disabled.events).containsExactly("batch:1");
        }
    }

    private static OneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch> harness(long timeout, Capture capture)
            throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<RowData, ArrowRowDataBatch>(
                new RowDataToArrowBatchOperator(TYPE, null, null, timeout));
        harness.setOutputCreator(ignored -> capture);
        harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
        return harness;
    }

    private static final class Capture implements Output<StreamRecord<ArrowRowDataBatch>> {
        private final DataOutputSerializer bytes = new DataOutputSerializer(256);
        private final List<String> events = new ArrayList<>();
        private final List<Long> timestamps = new ArrayList<>();
        private final RowDataSerializer serializer = new RowDataSerializer(TYPE);

        @Override
        public void collect(StreamRecord<ArrowRowDataBatch> record) {
            ArrowRowDataBatch batch = record.getValue();
            events.add("batch:" + batch.size());
            try {
                for (int row = 0; row < batch.size(); row++) {
                    RowData view = batch.rowView(row);
                    view.setRowKind(batch.rowKind(row));
                    serializer.serialize(view, bytes);
                    if (batch.hasTimestamp(row)) {
                        timestamps.add(batch.timestamp(row));
                    }
                }
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }

        @Override
        public void emitWatermark(Watermark mark) {
            events.add("watermark:" + mark.getTimestamp());
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus status) {}

        @Override
        public <X> void collect(OutputTag<X> tag, StreamRecord<X> record) {
            throw new AssertionError("side output");
        }

        @Override
        public void emitLatencyMarker(LatencyMarker marker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes attributes) {}

        @Override
        public void emitWatermark(WatermarkEvent watermark) {}

        @Override
        public void close() {}
    }
}
