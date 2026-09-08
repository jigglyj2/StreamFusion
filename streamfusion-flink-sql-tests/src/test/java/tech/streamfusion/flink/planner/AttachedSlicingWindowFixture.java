/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.*;

/** Fixed HOP attached-window MAX/COUNT partials, independent of the benchmark query. */
final class AttachedSlicingWindowFixture {
    static final RowType INPUT = SharedSlicingWindowFixture.INPUT;
    static final RowType FLINK_INPUT =
            RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType());
    static final RowType OUTPUT = RowType.of(
            new BigIntType(),
            new BigIntType(false),
            new BigIntType(false),
            new TimestampType(false, 3),
            new TimestampType(false, 3));

    private AttachedSlicingWindowFixture() {}

    static byte[] partial(long maximum, long count) {
        return ByteBuffer.allocate(45)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(new byte[] {'S', 'F', 'G', 'A', 6})
                .putLong(count)
                .putInt(2)
                .put((byte) 4)
                .put((byte) 1)
                .put((byte) 2)
                .putLong(maximum)
                .putLong(maximum < 0 ? -1 : 0)
                .put((byte) 1)
                .putLong(count)
                .array();
    }

    static RowType type(RowType grouped, boolean grouping) {
        return grouping
                ? grouped
                : RowType.of(grouped.getChildren()
                        .subList(1, grouped.getFieldCount())
                        .toArray(new org.apache.flink.table.types.logical.LogicalType[0]));
    }

    static String sql(boolean grouping) {
        String key = grouping ? "k, " : "";
        return "SELECT " + key + "MAX(n) AS m, COUNT(*) AS c, window_start, window_end FROM ("
                + "SELECT k, COUNT(*) AS n, window_start, window_end "
                + "FROM TABLE(HOP(TABLE local_window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) "
                + "GROUP BY k, window_start, window_end) GROUP BY " + key + "window_start, window_end";
    }

    static byte[] plan(boolean grouping) throws Exception {
        var original = NativePlan.parseFrom(SharedSlicingWindowFixture.plan());
        var node = original.getRoot().getCalc().getInput();
        var window = node.getWindowAggregate().toBuilder()
                .setPartialWindowsAreSlices(false)
                .setOutputSchema(SharedSlicingWindowFixture.schema(type(OUTPUT, grouping)))
                .clearAggregateCalls()
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_MAX)
                        .setInputIndex(0)
                        .setInputType(FlinkLogicalTypeProto.serialize(new BigIntType(false)))
                        .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType(false))))
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType(false))));
        if (!grouping) {
            window.clearGroupingIndices()
                    .setInputSchema(SharedSlicingWindowFixture.schema(type(INPUT, false)))
                    .setPartialAccumulatorIndex(0)
                    .setPartialWindowStartIndex(1)
                    .setPartialSliceEndIndex(2)
                    .setInput(SharedSlicingWindowFixture.calc(
                            2, window.getInput().getCalc().getInput(), 3));
        }
        return original.toBuilder()
                .setRoot(SharedSlicingWindowFixture.calc(
                        4, node.toBuilder().setWindowAggregate(window).build(), grouping ? 5 : 4))
                .build()
                .toByteArray();
    }
}
