/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.nativebridge.NativeGroupAggregateBridge;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.GroupAggregate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class ArrowGroupAggregateCDataBridgeTest {
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(true)}, new String[] {"bidder", "price"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false),
                new BigIntType(false),
                new BigIntType(true),
                new BigIntType(true),
                new BigIntType(true)
            },
            new String[] {"bidder", "bids", "spend", "minimum", "maximum"});

    @Test
    void returnsFlinkPerRecordAggregateChangelogDirectlyFromArrow() {
        long handle = NativeGroupAggregateBridge.create(plan(false), 128, 0, 127, NativeMemoryManager.unbounded());
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                        List.of(row(7, 10L), row(7, 20L), row(7, null), row(8, 5L)), INPUT_TYPE, allocator);
                ArrowRowDataBatch output =
                        ArrowGroupAggregateCDataBridge.execute(handle, input, null, false, OUTPUT_TYPE, allocator)) {
            assertThat(output.size()).isEqualTo(6);
            assertThat(output.rowKind(0)).isEqualTo(RowKind.INSERT);
            assertThat(output.rowKind(1)).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(output.rowKind(2)).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(output.rowKind(3)).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(output.rowKind(4)).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(output.rowKind(5)).isEqualTo(RowKind.INSERT);
            assertThat(output.rowView(4).getLong(0)).isEqualTo(7);
            assertThat(output.rowView(4).getLong(1)).isEqualTo(3);
            assertThat(output.rowView(4).getLong(2)).isEqualTo(30);
            assertThat(output.rowView(4).getLong(3)).isEqualTo(10);
            assertThat(output.rowView(4).getLong(4)).isEqualTo(20);
        } finally {
            NativeGroupAggregateBridge.destroy(handle);
        }
    }

    @Test
    void consumesRetractionsAndDeletesAnEmptyGroup() {
        long handle = NativeGroupAggregateBridge.create(plan(true), 128, 0, 127, NativeMemoryManager.unbounded());
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                ArrowRowDataBatch inserted = ArrowRowDataBatch.transpose(
                                List.of(row(7, 10L), row(7, 20L)), INPUT_TYPE, allocator)
                        .withRowKinds(new RowKind[] {RowKind.INSERT, RowKind.INSERT});
                ArrowRowDataBatch ignored =
                        ArrowGroupAggregateCDataBridge.execute(handle, inserted, null, true, OUTPUT_TYPE, allocator);
                ArrowRowDataBatch retracted = ArrowRowDataBatch.transpose(
                                List.of(row(7, 20L), row(7, 10L)), INPUT_TYPE, allocator)
                        .withRowKinds(new RowKind[] {RowKind.DELETE, RowKind.DELETE});
                ArrowRowDataBatch output =
                        ArrowGroupAggregateCDataBridge.execute(handle, retracted, null, true, OUTPUT_TYPE, allocator)) {
            assertThat(output.size()).isEqualTo(3);
            assertThat(output.rowKind(0)).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(output.rowKind(1)).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(output.rowKind(2)).isEqualTo(RowKind.DELETE);
            assertThat(output.rowView(2).getLong(1)).isEqualTo(1);
            assertThat(output.rowView(2).getLong(2)).isEqualTo(10);
        } finally {
            NativeGroupAggregateBridge.destroy(handle);
        }
    }

    private static GenericRowData row(long bidder, Long price) {
        return GenericRowData.of(bidder, price);
    }

    private static byte[] plan(boolean changelog) {
        GroupAggregate.Builder aggregate = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .setGenerateUpdateBefore(true)
                .setInputChangelog(changelog);
        aggregate.addAggregateCalls(call(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR, false, changelog));
        aggregate.addAggregateCalls(call(AggregateFunction.AGGREGATE_FUNCTION_SUM, true, changelog));
        aggregate.addAggregateCalls(call(AggregateFunction.AGGREGATE_FUNCTION_MIN, true, changelog));
        aggregate.addAggregateCalls(call(AggregateFunction.AGGREGATE_FUNCTION_MAX, true, changelog));
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static AggregateCall call(AggregateFunction function, boolean input, boolean retractable) {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setNullable(function != AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        AggregateCall.Builder call = AggregateCall.newBuilder()
                .setFunction(function)
                .setOutputType(bigint)
                .setRetractable(retractable);
        if (input) {
            call.setInputIndex(1).setInputType(bigint);
        }
        return call.build();
    }
}
