/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.*;

/** Explicit shared binding fixture; ordinary planner admission remains independently gated. */
final class SharedProcessingWindowFixture {
    static final RowType INPUT =
            RowType.of(new BigIntType(), new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3));
    static final RowType OUTPUT = SharedSlicingWindowFixture.OUTPUT;

    private SharedProcessingWindowFixture() {}

    static byte[] plan(int gap) {
        var input = Operator.newBuilder().setPlanNodeId(1).setInput(Input.getDefaultInstance());
        var window = WindowAggregate.newBuilder()
                .setInput(input)
                .setProcessingTime(true)
                .setKind(WindowKind.WINDOW_KIND_TUMBLE)
                .setSizeMillis(gap)
                .setTimeAttributeIndex(1)
                .setShiftTimeZone("UTC")
                .addGroupingIndices(0)
                .setInputSchema(SharedSlicingWindowFixture.schema(INPUT))
                .setOutputSchema(SharedSlicingWindowFixture.schema(OUTPUT))
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType(false))))
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END);
        var owner = Operator.newBuilder()
                .setPlanNodeId(3)
                .setWindowAggregate(window)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(SharedSlicingWindowFixture.calc(4, owner, 4))
                .build()
                .toByteArray();
    }

    static StreamFusionNativeRegionOperatorFactory factory(int gap) {
        return new StreamFusionNativeRegionOperatorFactory(
                List.of(INPUT),
                OUTPUT,
                plan(gap),
                List.of(3L),
                List.of(NativeExchangePlanSerializer.singleton(INPUT)),
                new NativeLocalWindowResources(Map.of(
                        3L,
                        new FlinkOperatorMemoryShare(
                                1, 1, Set.of(ManagedMemoryUseCase.OPERATOR, ManagedMemoryUseCase.STATE_BACKEND)))));
    }
}
