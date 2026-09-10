/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.GroupAggregate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Append-only VARCHAR extrema mixed with filtered DISTINCT counts and composite grouping. */
final class StringAggregateFixture {
    private StringAggregateFixture() {}

    static byte[] plan() {
        var bigint = LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .setNullable(true)
                .build();
        var string = LogicalType.newBuilder()
                .setVarchar(EmptyType.getDefaultInstance())
                .setNullable(true)
                .build();
        var group = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setPlanNodeId(1).setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .addGroupingIndices(1)
                .setGenerateUpdateBefore(true)
                .setInputChangelog(false);
        for (var function : List.of(
                AggregateFunction.AGGREGATE_FUNCTION_MIN,
                AggregateFunction.AGGREGATE_FUNCTION_MAX,
                AggregateFunction.AGGREGATE_FUNCTION_MAX)) {
            var call = AggregateCall.newBuilder()
                    .setFunction(function)
                    .setInputIndex(2)
                    .setInputType(string)
                    .setOutputType(string);
            if (group.getAggregateCallsCount() == 2) call.setFilterIndex(3);
            group.addAggregateCalls(call);
        }
        for (boolean filtered : List.of(false, true)) {
            var call = AggregateCall.newBuilder()
                    .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT)
                    .setInputIndex(4)
                    .setInputType(bigint)
                    .setOutputType(bigint.toBuilder().setNullable(false))
                    .setDistinct(true)
                    .setRetractable(false);
            if (filtered) call.setFilterIndex(3);
            group.addAggregateCalls(call);
        }
        group.addAggregateCalls(AggregateCall.newBuilder()
                .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                .setOutputType(bigint.toBuilder().setNullable(false))
                .setRetractable(false));
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder().setPlanNodeId(3).setGroupAggregate(group))
                .build()
                .toByteArray();
    }
}
