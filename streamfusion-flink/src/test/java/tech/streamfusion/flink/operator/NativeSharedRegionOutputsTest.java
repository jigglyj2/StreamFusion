/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.*;

class NativeSharedRegionOutputsTest {
    @Test
    void broadcastsEachCompleteWaveOnceIncludingRepeatedRestoredWatermarks() throws Exception {
        var outputs = new NativeSharedRegionOutputs(SharedNativeRegionRuntimeTest.plan(), 2);
        for (long time : List.of(500L, 500L, 700L)) {
            assertThat(outputs.watermark(12, time)).isFalse();
            assertThat(outputs.watermark(11, time)).isFalse();
            assertThat(outputs.watermark(13, time)).isTrue();
        }
        for (var status : List.of(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE)) {
            assertThat(outputs.status(13, status)).isFalse();
            assertThat(outputs.status(11, status)).isTrue();
        }
        assertThat(outputs.outputTag(1)).isSameAs(outputs.outputTag(1));
        assertThat(outputs.outputTag(1))
                .isEqualTo(SharedNativeRegionRuntimeTest.factory().outputTag(1));
        assertThatThrownBy(() -> NativeSharedRegionOutputs.tag(0)).hasMessageContaining("main output");
    }

    @Test
    void mismatchedOrOverlappingControlWavesFailInsteadOfBroadcastingAnIncorrectFrontier() throws Exception {
        var outputs = new NativeSharedRegionOutputs(SharedNativeRegionRuntimeTest.plan(), 2);
        outputs.watermark(11, 100);
        assertThatThrownBy(() -> outputs.watermark(13, 200)).hasMessageContaining("disagree");
        var overlap = new NativeSharedRegionOutputs(SharedNativeRegionRuntimeTest.plan(), 2);
        overlap.status(11, WatermarkStatus.IDLE);
        assertThatThrownBy(() -> overlap.status(11, WatermarkStatus.ACTIVE)).hasMessageContaining("before all exits");
    }

    @Test
    void admitsOnlyOneInputUnaryRegionsWithMatchingWindowClockAncestry() throws Exception {
        var plan = SharedNativeRegionRuntimeTest.plan();
        assertThatThrownBy(() -> new NativeSharedRegionOutputs(plan, 1)).hasMessageContaining("distinct exits");
        var window = Operator.newBuilder()
                .setPlanNodeId(12)
                .setWindowAggregate(WindowAggregate.newBuilder()
                        .setInput(Operator.newBuilder().setInput(Input.newBuilder())));
        var changed = plan.toBuilder()
                .setStages(1, plan.getStages(1).toBuilder().setOperator(window))
                .build();
        assertThatThrownBy(() -> new NativeSharedRegionOutputs(changed, 2))
                .hasMessageContaining("different restored window-clock paths");
        var common = plan.toBuilder()
                .setStages(0, plan.getStages(0).toBuilder().setOperator(window.setPlanNodeId(11)))
                .build();
        new NativeSharedRegionOutputs(common, 2);
        var union = Operator.newBuilder()
                .setPlanNodeId(11)
                .setUnion(Union.newBuilder().addInputs(Operator.newBuilder().setInput(Input.newBuilder())));
        var unionPlan = plan.toBuilder()
                .setStages(0, plan.getStages(0).toBuilder().setOperator(union))
                .build();
        assertThatThrownBy(() -> new NativeSharedRegionOutputs(unionPlan, 2)).hasMessageContaining("diverging unary");
        var twoInputs = plan.toBuilder()
                .setInputCount(2)
                .setStages(
                        2,
                        plan.getStages(2).toBuilder()
                                .clearInputs()
                                .addInputs(
                                        NativeRegionInputReference.newBuilder().setExternalInput(1)))
                .addOutputStageIds(12)
                .build();
        assertThatThrownBy(() -> new NativeSharedRegionOutputs(twoInputs, 3))
                .hasMessageContaining("one external input frontier");
    }
}
