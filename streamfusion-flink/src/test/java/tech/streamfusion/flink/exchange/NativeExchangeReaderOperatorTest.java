/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class NativeExchangeReaderOperatorTest {
    @Test
    void restoresFlinkRecordEnvelopeAndForwardsControls() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128);
        GenericRowData deleted = GenericRowData.of(42);
        deleted.setRowKind(RowKind.DELETE);
        NativeExchangeFrame frame;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch batch = ArrowExchangeBatch.transpose(
                        List.of(new StreamRecord<RowData>(deleted, 123L)), rowType, allocator)) {
            frame = ArrowExchangeCDataBridge.route(plan, batch, allocator).get(0);
        }

        NativeExchangeReaderOperator operator = new NativeExchangeReaderOperator(rowType, plan);
        try (OneInputStreamOperatorTestHarness<NativeExchangeFrame, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new RowDataSerializer(rowType));
            harness.open();
            harness.processElement(new StreamRecord<>(frame));
            StreamRecord<RowData> result =
                    (StreamRecord<RowData>) harness.getOutput().poll();
            assertThat(result.getValue().getInt(0)).isEqualTo(42);
            assertThat(result.getValue().getRowKind()).isEqualTo(RowKind.DELETE);
            assertThat(result.hasTimestamp()).isTrue();
            assertThat(result.getTimestamp()).isEqualTo(123L);

            harness.processWatermark(new Watermark(200L));
            assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(200L));
        }
    }
}
