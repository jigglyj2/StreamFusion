/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Queue;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class NativeExchangeWriterOperatorTest {
    @Test
    void routesAnExistingArrowBatchBeforeFollowingControls() throws Exception {
        RowType rowType = RowType.of(new IntType());
        NativeExchangeWriterOperator operator =
                new NativeExchangeWriterOperator(rowType, new byte[] {1}, (plan, batch, allocator, memoryManager) -> {
                    assertThat(batch.size()).isEqualTo(1);
                    assertThat(batch.root().getFieldVectors()).hasSize(3);
                    assertThat(memoryManager.limit()).isPositive();
                    assertThat(allocator.getLimit()).isEqualTo(memoryManager.limit());
                    return List.of(new NativeExchangeFrame(7, new byte[] {2}, new byte[] {3}));
                });
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(
                                List.of(GenericRowData.of(42)), rowType, allocator)
                        .withEnvelope(new RowKind[] {RowKind.DELETE}, new boolean[] {true}, new long[] {100});
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, NativeExchangeFrame> harness =
                        new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.open();
            harness.processElement(new StreamRecord<>(batch));
            harness.processWatermark(new Watermark(200L));

            Queue<Object> output = harness.getOutput();
            assertThat(output).hasSize(2);
            assertThat(output.poll()).isInstanceOf(StreamRecord.class);
            assertThat(output.poll()).isEqualTo(new Watermark(200L));
        }
    }
}
