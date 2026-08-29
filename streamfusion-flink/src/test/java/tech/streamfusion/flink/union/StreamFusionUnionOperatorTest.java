/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.union;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

class StreamFusionUnionOperatorTest {
    @Test
    void forwardsArrowBatchesInArrivalOrderBeforeCombinedWatermark() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch first = batch(allocator, rowType, 10, RowKind.INSERT, 100);
                ArrowRowDataBatch second = batch(allocator, rowType, 20, RowKind.UPDATE_AFTER, 200);
                ArrowRowDataBatch third = batch(allocator, rowType, 30, RowKind.DELETE, 300);
                MultiInputStreamOperatorTestHarness<ArrowRowDataBatch> harness =
                        new MultiInputStreamOperatorTestHarness<>(new StreamFusionUnionOperatorFactory(2))) {
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();
            harness.processElement(0, new StreamRecord<>(first));
            harness.processElement(1, new StreamRecord<>(second));
            harness.processElement(0, new StreamRecord<>(third));
            harness.processWatermark(0, new Watermark(50));

            assertThat(summaries(harness.getOutput())).containsExactly("+I:10@100", "+U:20@200", "-D:30@300");
            assertThat(harness.getOutput()).noneMatch(Watermark.class::isInstance);

            harness.processWatermark(1, new Watermark(60));
            assertThat(harness.getOutput())
                    .filteredOn(Watermark.class::isInstance)
                    .containsExactly(new Watermark(50));
        }
    }

    private static ArrowRowDataBatch batch(
            RootAllocator allocator, RowType rowType, int value, RowKind kind, long timestamp) {
        return ArrowRowDataBatch.transpose(List.of(GenericRowData.of(value)), rowType, allocator)
                .withEnvelope(new RowKind[] {kind}, new boolean[] {true}, new long[] {timestamp});
    }

    private static List<String> summaries(Queue<Object> output) {
        List<String> summaries = new ArrayList<>();
        for (Object event : output) {
            if (event instanceof StreamRecord<?>) {
                ArrowRowDataBatch batch = (ArrowRowDataBatch) ((StreamRecord<?>) event).getValue();
                summaries.add(
                        batch.rowKind(0).shortString() + ":" + batch.rowView(0).getInt(0) + "@" + batch.timestamp(0));
            }
        }
        return summaries;
    }
}
