/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.*;

/** Flink SQL and native Calc -> merging COUNT -> Calc fixture, without an admission bypass. */
final class SharedSessionWindowFixture {
    static final RowType INPUT = RowType.of(new BigIntType(), new TimestampType(3));
    static final RowType OUTPUT = RowType.of(
            new BigIntType(), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3));
    static final String SQL = "SELECT k, COUNT(*) AS n, window_start, window_end FROM TABLE("
            + "SESSION(TABLE local_window_input PARTITION BY k, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
            + "GROUP BY k, window_start, window_end";

    private SharedSessionWindowFixture() {}

    static RowData row(long timestamp) {
        return GenericRowData.of(1L, TimestampData.fromEpochMillis(timestamp));
    }

    static byte[] plan() {
        var input = Operator.newBuilder()
                .setPlanNodeId(1)
                .setInput(Input.newBuilder())
                .build();
        var window = Operator.newBuilder()
                .setPlanNodeId(3)
                .setWindowAggregate(WindowAggregate.newBuilder()
                        .setInput(SharedSlicingWindowFixture.calc(2, input, 2))
                        .setInputSchema(SharedSlicingWindowFixture.schema(INPUT))
                        .setOutputSchema(SharedSlicingWindowFixture.schema(OUTPUT))
                        .addGroupingIndices(0)
                        .addAggregateCalls(AggregateCall.newBuilder()
                                .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                                .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType(false))))
                        .setTimeAttributeIndex(1)
                        .setKind(WindowKind.WINDOW_KIND_SESSION)
                        .setSizeMillis(10000)
                        .setShiftTimeZone("UTC")
                        .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                        .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(SharedSlicingWindowFixture.calc(4, window, 4))
                .build()
                .toByteArray();
    }
}
