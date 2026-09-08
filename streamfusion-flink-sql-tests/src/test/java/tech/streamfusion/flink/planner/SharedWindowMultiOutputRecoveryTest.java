/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.*;

/** Runs the same generated Flink recovery cases against both exits of one physical window owner. */
class SharedWindowMultiOutputRecoveryTest extends SharedWindowRuntimeRecoveryTest {
    @Override
    protected KeyedNativeMetricHarness region(
            boolean attached, boolean rocks, OperatorSubtaskState restore, int parallelism, int subtask)
            throws Exception {
        var plan = SharedWindowMultiOutputFixture.plan(attached);
        var output = attached ? AttachedSlicingWindowFixture.OUTPUT : SharedSlicingWindowFixture.OUTPUT;
        var factory = StreamFusionNativeRegionOperatorFactory.shared(
                List.of(SharedSlicingWindowFixture.INPUT),
                List.of(output, output),
                plan.toByteArray(),
                List.of(3L),
                List.of(NativeExchangePlanSerializer.singleton(SharedSlicingWindowFixture.INPUT)),
                NativeLocalWindowResources.NONE);
        return new KeyedNativeMetricHarness(rocks, factory, 1, List.of(output, output), restore, parallelism, subtask);
    }
}
