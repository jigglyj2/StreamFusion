/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import tech.streamfusion.proto.plan.v1.*;

/** One window owner, with its result exposed before and after an identity Calc. */
final class SharedWindowMultiOutputFixture {
    private SharedWindowMultiOutputFixture() {}

    static NativeRegionPlan plan(boolean attached) throws Exception {
        var tree = NativePlan.parseFrom(
                        attached ? AttachedSlicingWindowFixture.plan(true) : SharedSlicingWindowFixture.plan())
                .getRoot();
        var window = tree.getCalc().getInput();
        var first = window.getWindowAggregate().getInput();
        var input = Operator.newBuilder().setInput(Input.newBuilder()).build();
        var plan = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(1)
                .addOutputStageIds(3)
                .addOutputStageIds(4)
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(first.toBuilder()
                                .setCalc(first.getCalc().toBuilder().setInput(input)))
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0)))
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(window.toBuilder()
                                .setWindowAggregate(
                                        window.getWindowAggregate().toBuilder().setInput(input)))
                        .addInputs(NativeRegionInputReference.newBuilder().setStageId(2)))
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(tree.toBuilder()
                                .setCalc(tree.getCalc().toBuilder().setInput(input)))
                        .addInputs(NativeRegionInputReference.newBuilder().setStageId(3)))
                .build();
        return plan;
    }
}
