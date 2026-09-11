/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                    return List.of(frame(7, 1));
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

    @Test
    void failuresRetainLogicalInputAndAttemptedOutputCounts() throws Exception {
        var type = RowType.of(new IntType());
        for (boolean routingFailure : List.of(false, true)) {
            var failure = new IllegalStateException(routingFailure ? "routing failed" : "consumer failed");
            var operator = new NativeExchangeWriterOperator(type, new byte[] {1}, (plan, batch, allocator, memory) -> {
                if (routingFailure) throw failure;
                return List.of(frame(0, 3), frame(1, 4), frame(2, 5));
            });
            var accepted = new java.util.ArrayList<NativeExchangeFrame>();
            try (var allocator = new RootAllocator();
                    var batch = ArrowRowDataBatch.transpose(
                            java.util.stream.IntStream.range(0, 12)
                                    .mapToObj(value -> GenericRowData.of(value))
                                    .collect(java.util.stream.Collectors.toList()),
                            type,
                            allocator);
                    var harness =
                            new OneInputStreamOperatorTestHarness<ArrowRowDataBatch, NativeExchangeFrame>(operator)) {
                harness.setOutputCreator(ignored ->
                        new org.apache.flink.streaming.util.CollectorOutput<NativeExchangeFrame>(
                                new java.util.ArrayList<>()) {
                            private int attempted;

                            @Override
                            public void collect(StreamRecord<NativeExchangeFrame> record) {
                                operator.getMetricGroup()
                                        .getIOMetricGroup()
                                        .getNumRecordsOutCounter()
                                        .inc();
                                if (++attempted == 2) throw failure;
                                accepted.add(record.getValue());
                            }
                        });
                harness.open();
                var io = operator.getMetricGroup().getIOMetricGroup();
                io.getNumRecordsInCounter().inc();
                assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(batch)))
                        .isSameAs(failure);
                assertThat(io.getNumRecordsInCounter().getCount()).isEqualTo(12);
                assertThat(io.getNumRecordsOutCounter().getCount()).isEqualTo(routingFailure ? 0 : 7);
                assertThat(accepted).hasSize(routingFailure ? 0 : 1);
            }
        }
    }

    private static NativeExchangeFrame frame(int keyGroup, int rows) {
        return new NativeExchangeFrame(
                keyGroup,
                NativeExchangeRowCountTest.header(rows, org.apache.arrow.flatbuf.MessageHeader.RecordBatch, 0),
                new byte[0]);
    }
}
