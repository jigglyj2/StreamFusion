/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class NativeExchangeReaderOperatorTest {
    @Test
    void leavesFlinkControlEventsOnTheFlinkPath() throws Exception {
        RowType rowType = RowType.of(new IntType(false));
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128);
        try (OneInputStreamOperatorTestHarness<NativeExchangeFrame, ArrowRowDataBatch> harness =
                new OneInputStreamOperatorTestHarness<>(new NativeExchangeReaderOperator(rowType, plan))) {
            harness.open();
            harness.processWatermark(new Watermark(200L));
            assertThat(harness.getOutput()).containsExactly(new Watermark(200L));
        }
    }
}
