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
        var rowtime = new TimestampType(false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3);
        var raw = RowType.of(
                new BigIntType(), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3));
        var strategy = new org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy(
                SharedSlicingWindowFixture.hop(), rowtime, 2, 3);
        var calls = new org.apache.calcite.rel.core.AggregateCall[] {
            SharedSlicingWindowFixture.call(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.MAX, java.util.List.of(1), new BigIntType(false)),
            SharedSlicingWindowFixture.call(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT, java.util.List.of(), new BigIntType(false))
        };
        var input = type(INPUT, grouping);
        var output = type(OUTPUT, grouping);
        return SharedSlicingWindowFixture.compose(
                tech.streamfusion.flink.window.StreamFusionGlobalWindowAggregateTranslator.createStagePlan(
                        raw,
                        input,
                        output,
                        grouping ? 1 : 0,
                        calls,
                        strategy,
                        SharedSlicingWindowFixture.properties(),
                        false,
                        SharedSlicingWindowFixture.config()),
                input.getFieldCount(),
                output.getFieldCount());
    }
}
