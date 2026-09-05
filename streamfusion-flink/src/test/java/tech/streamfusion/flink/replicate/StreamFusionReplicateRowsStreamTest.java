/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.replicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LogicalType;

class StreamFusionReplicateRowsStreamTest {
    @Test
    void streamsLargeExpansionInBoundedBatchesAndReleasesOnce() {
        RowType inputType = RowType.of(new BigIntType(false), new IntType(false));
        RowType outputType = RowType.of(new BigIntType(false), new IntType(false), new IntType(false));
        byte[] plan = StreamFusionReplicateRowsPlan.create(
                input(
                        0,
                        LogicalType.newBuilder()
                                .setNullable(false)
                                .setBigint(EmptyType.getDefaultInstance())
                                .build()),
                List.of(input(
                        1,
                        LogicalType.newBuilder()
                                .setNullable(false)
                                .setInteger(EmptyType.getDefaultInstance())
                                .build())));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeExecutionContext context = new NativeExecutionContext(plan, TestingNativeMemoryManager.create());
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                                List.of(GenericRowData.of(16_385L, 7)), inputType, allocator)
                        .withEnvelope(new RowKind[] {RowKind.DELETE}, new boolean[] {true}, new long[] {42L})) {
            ArrowCDataBridge.ReusableExecution execution =
                    new ArrowCDataBridge.ReusableExecution(context, outputType, allocator);
            try (ArrowCDataBridge.NativeOutputStream stream = execution.executeStream(input);
                    NativeCalcResult first = stream.nextWithSelection();
                    NativeCalcResult second = stream.nextWithSelection()) {
                assertThat(first.batch().size()).isEqualTo(16_384);
                assertThat(second.batch().size()).isOne();
                assertThat(first.batch().rowView(0).getInt(2)).isEqualTo(7);
                assertThat(second.batch().rowView(0).getInt(2)).isEqualTo(7);
                assertThat(first.inputRow(16_383)).isZero();
                assertThat(second.inputRow(0)).isZero();
                first.selectEnvelopeFrom(input);
                second.selectEnvelopeFrom(input);
                assertThat(first.batch().rowKind(16_383)).isEqualTo(RowKind.DELETE);
                assertThat(second.batch().rowKind(0)).isEqualTo(RowKind.DELETE);
                assertThat(first.batch().timestamp(16_383)).isEqualTo(42L);
                assertThat(second.batch().timestamp(0)).isEqualTo(42L);
                assertThat(stream.nextWithSelection()).isNull();
                stream.close();
            }
        }
    }

    private static Expression input(int index, LogicalType type) {
        return Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(index).setType(type))
                .build();
    }
}
