/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Queue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeExchangeWriterOperatorTest {
    @Test
    void flushesBufferedArrowDataBeforeWatermarks() throws Exception {
        NativeExchangeWriterOperator operator = new NativeExchangeWriterOperator(
                RowType.of(new IntType()), new byte[] {1}, 8, (plan, batch, allocator, memoryManager) -> {
                    assertThat(batch.size()).isEqualTo(1);
                    return List.of(new NativeExchangeFrame(7, new byte[] {2}, new byte[] {3}));
                });
        try (OneInputStreamOperatorTestHarness<RowData, NativeExchangeFrame> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            harness.processElement(new StreamRecord<>(GenericRowData.of(42), 100L));
            harness.processWatermark(new Watermark(200L));

            Queue<Object> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.poll()).isInstanceOf(StreamRecord.class);
            assertThat(output.poll()).isEqualTo(new Watermark(200L));
        }
    }

    @Test
    void flushesBeforeCheckpointAndBoundedCompletion() throws Exception {
        int[] routedBatches = {0};
        NativeExchangeWriterOperator operator = new NativeExchangeWriterOperator(
                RowType.of(new IntType()), new byte[] {1}, 8, (plan, batch, allocator, memoryManager) -> {
                    routedBatches[0]++;
                    return List.of(new NativeExchangeFrame(0, new byte[0], new byte[0]));
                });
        try (OneInputStreamOperatorTestHarness<RowData, NativeExchangeFrame> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            harness.processElement(new StreamRecord<>(GenericRowData.of(1)));
            operator.prepareSnapshotPreBarrier(11L);
            harness.processElement(new StreamRecord<>(GenericRowData.of(2)));
            operator.endInput();

            assertThat(routedBatches[0]).isEqualTo(2);
        }
    }
}
