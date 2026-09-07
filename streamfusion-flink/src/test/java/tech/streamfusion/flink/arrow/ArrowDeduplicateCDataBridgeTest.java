/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.nativebridge.NativeDeduplicateBridge;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LongLiteral;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class ArrowDeduplicateCDataBridgeTest {
    private static final RowType ROW_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new BigIntType(false), new TimestampType(false, TimestampKind.ROWTIME, 3)
            },
            new String[] {"bidder", "auction", "dateTime"});

    @Test
    void returnsArrowRowsAndNativeChangelogEnvelope() {
        long handle = NativeDeduplicateBridge.create(plan(), 128, 0, 127, TestingNativeMemoryManager.create());
        try (RootAllocator allocator = new RootAllocator(64L << 20)) {
            List<RowData> rows = List.of(row(7, 9, 1_000), row(7, 9, 2_000), row(7, 9, 1_500));
            try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, ROW_TYPE, allocator)
                            .withEnvelope(
                                    new RowKind[] {RowKind.INSERT, RowKind.INSERT, RowKind.INSERT},
                                    new boolean[] {true, true, true},
                                    new long[] {10, 20, 15});
                    NativeArrowDeduplicateResult result =
                            ArrowDeduplicateCDataBridge.executeArrow(handle, input, null, ROW_TYPE, allocator)) {
                ArrowRowDataBatch output = result.selectEnvelopeFrom(input);
                assertThat(output.size()).isEqualTo(2);
                assertThat(output.rowView(0).getTimestamp(2, 3).getMillisecond())
                        .isEqualTo(1_000);
                assertThat(output.rowView(1).getTimestamp(2, 3).getMillisecond())
                        .isEqualTo(2_000);
                assertThat(output.rowKind(0)).isEqualTo(RowKind.INSERT);
                assertThat(output.rowKind(1)).isEqualTo(RowKind.UPDATE_AFTER);
                assertThat(output.timestamp(0)).isEqualTo(10);
                assertThat(output.timestamp(1)).isEqualTo(20);
            }
        } finally {
            NativeDeduplicateBridge.destroy(handle);
        }
    }

    private static GenericRowData row(long bidder, long auction, long timestamp) {
        return GenericRowData.of(bidder, auction, TimestampData.fromEpochMillis(timestamp));
    }

    @Test
    void composedTreeRunsThroughTheProductionEdgeAndCountsPreFilterStateOutput(@TempDir Path directory)
            throws Exception {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (int index : new int[] {1, 0, 2, 3, 4}) {
            calc.addProjections(reference(index));
        }
        calc.setCondition(Expression.newBuilder()
                .setComparison(Comparison.newBuilder()
                        .setLeft(reference(0))
                        .setRight(Expression.newBuilder()
                                .setLongLiteral(LongLiteral.newBuilder().setValue(7)))
                        .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)));
        byte[] tail = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
        byte[] composed = StreamFusionNativeRegionTranslator.compose(List.of(plan(), tail));
        for (boolean rocks : List.of(false, true)) {
            var memory = TestingNativeMemoryManager.create();
            long handle = rocks
                    ? NativeDeduplicateBridge.createRocksDb(
                            composed, 128, 0, 127, directory.resolve("rocks"), 32L << 20, memory)
                    : NativeDeduplicateBridge.create(composed, 128, 0, 127, memory);
            try (RootAllocator allocator = new RootAllocator(64L << 20)) {
                try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                                        List.of(row(7, 9, 1000), row(7, 9, 2000), row(4, 3, 1500)), ROW_TYPE, allocator)
                                .withEnvelope(
                                        new RowKind[] {RowKind.INSERT, RowKind.INSERT, RowKind.INSERT},
                                        new boolean[] {true, true, true},
                                        new long[] {10, 20, 15});
                        NativeArrowDeduplicateResult result =
                                ArrowDeduplicateCDataBridge.executeArrow(handle, input, null, ROW_TYPE, allocator)) {
                    ArrowRowDataBatch output = result.selectEnvelopeFrom(input);
                    assertThat(output.size()).isEqualTo(2);
                    assertThat(output.rowView(0).getLong(0)).isEqualTo(9);
                    assertThat(output.rowView(0).getLong(1)).isEqualTo(7);
                    assertThat(output.rowKind(0)).isEqualTo(RowKind.INSERT);
                    assertThat(output.rowKind(1)).isEqualTo(RowKind.UPDATE_AFTER);
                    assertThat(output.timestamp(0)).isEqualTo(10);
                    assertThat(output.timestamp(1)).isEqualTo(20);
                }
                assertThat(NativeDeduplicateBridge.metricSnapshot(handle)).containsExactly(1, 3, 2, 2, 3, 3, 3, 0, 3);
                try (ArrowRowDataBatch input =
                                ArrowRowDataBatch.transpose(List.of(row(4, 3, 2500)), ROW_TYPE, allocator);
                        NativeArrowDeduplicateResult result =
                                ArrowDeduplicateCDataBridge.executeArrow(handle, input, null, ROW_TYPE, allocator)) {
                    assertThat(result.size()).isZero();
                }
                assertThat(NativeDeduplicateBridge.metricSnapshot(handle)).containsExactly(1, 4, 2, 2, 4, 4, 3, 0, 4);
            } finally {
                NativeDeduplicateBridge.destroy(handle);
            }
        }
    }

    private static Expression reference(int index) {
        return Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(index))
                .build();
    }

    private static byte[] plan() {
        Deduplicate deduplicate = Deduplicate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addKeyIndices(0)
                .addKeyIndices(1)
                .setOrderIndex(2)
                .setKeepLast(true)
                .setGenerateInsert(true)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setDeduplicate(deduplicate))
                .build()
                .toByteArray();
    }
}
