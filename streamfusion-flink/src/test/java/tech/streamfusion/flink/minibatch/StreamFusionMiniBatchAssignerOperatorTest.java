/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.minibatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

class StreamFusionMiniBatchAssignerOperatorTest {
    private static final RowType ROW_TYPE = RowType.of(new BigIntType(false));

    @Test
    void rowTimeCoalescesWatermarksAndFlushesTheBufferedWatermarkOnFinish() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch batch = batch(allocator);
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new StreamFusionArrowRowTimeMiniBatchAssignerOperator(5))) {
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();
            harness.processElement(new StreamRecord<>(batch));
            harness.processWatermark(new Watermark(2));
            harness.processWatermark(new Watermark(4));
            harness.processWatermark(new Watermark(10));
            harness.processWatermark(new Watermark(12));
            harness.getOperator().finish();

            assertThat(watermarks(harness.getOutput()))
                    .containsExactly(new Watermark(4), new Watermark(10), new Watermark(12));
        }
    }

    @Test
    void processingTimeUsesFlinkBatchBoundariesAndForwardsOnlyTheTerminalUpstreamWatermark() throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch batch = batch(allocator);
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new StreamFusionArrowProcTimeMiniBatchAssignerOperator(5))) {
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();
            harness.setProcessingTime(6);
            harness.processElement(new StreamRecord<>(batch));
            harness.processWatermark(new Watermark(8));
            harness.setProcessingTime(12);
            harness.processWatermark(Watermark.MAX_WATERMARK);

            assertThat(watermarks(harness.getOutput()))
                    .containsExactly(new Watermark(5), new Watermark(10), Watermark.MAX_WATERMARK);
        }
    }

    private static ArrowRowDataBatch batch(RootAllocator allocator) {
        return ArrowRowDataBatch.transpose(
                List.of(GenericRowData.of(1L), GenericRowData.of(2L), GenericRowData.of(3L)), ROW_TYPE, allocator);
    }

    private static List<Watermark> watermarks(Queue<Object> output) {
        return output.stream()
                .filter(Watermark.class::isInstance)
                .map(Watermark.class::cast)
                .collect(Collectors.toList());
    }
}
