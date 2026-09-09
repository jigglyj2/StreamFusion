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

/** Filtered and unfiltered DISTINCT count plan shared by runtime and recovery tests. */
final class DistinctCountFixture {
    private DistinctCountFixture() {}

    static byte[] plan() {
        return plan(false);
    }

    static byte[] plan(boolean appendOnly) {
        var bigint = LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .setNullable(true)
                .build();
        var group = GroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setPlanNodeId(1).setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .setGenerateUpdateBefore(true)
                .setInputChangelog(!appendOnly);
        for (boolean filtered : List.of(false, true)) {
            var call = AggregateCall.newBuilder()
                    .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT)
                    .setInputIndex(1)
                    .setInputType(bigint)
                    .setOutputType(bigint.toBuilder().setNullable(false))
                    .setDistinct(true)
                    .setRetractable(!appendOnly);
            if (filtered) call.setFilterIndex(2);
            group.addAggregateCalls(call);
        }
        group.addAggregateCalls(AggregateCall.newBuilder()
                .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                .setOutputType(bigint.toBuilder().setNullable(false))
                .setRetractable(!appendOnly));
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder().setPlanNodeId(3).setGroupAggregate(group))
                .build()
                .toByteArray();
    }
}
