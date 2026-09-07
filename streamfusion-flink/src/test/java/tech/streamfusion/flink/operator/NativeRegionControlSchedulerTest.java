/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativeControlCapabilities;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;
import tech.streamfusion.proto.plan.v1.NativeStageControl;
import tech.streamfusion.proto.plan.v1.NativeStageControlCapability;

class NativeRegionControlSchedulerTest {
    @Test
    void advancesBranchControlsBeforeTheRootAndCoalescesEachArrival() throws Exception {
        var events = new ArrayList<String>();
        var requests = new ArrayList<NativeControlInvocation>();
        var scheduler = scheduler(
                request -> {
                    requests.add(NativeControlInvocation.parseFrom(request));
                    events.add("native");
                    events.add("buffered-output");
                },
                events);
        scheduler.watermark(0, 200);
        assertThat(requests).isEmpty();
        scheduler.status(1, WatermarkStatus.IDLE);
        assertThat(ids(requests.get(0))).containsExactly(3L);
        assertThat(events).doesNotContain("root-watermark:200");
        events.clear();
        scheduler.watermark(2, 50);
        assertThat(ids(requests.get(1))).containsExactly(4L, 2L, 1L);
        assertThat(events).containsExactly("native", "buffered-output", "root-watermark:50");
        scheduler.watermark(2, 50);
        assertThat(requests).hasSize(2);
        scheduler.beforeCheckpoint(9);
        assertThat(ids(requests.get(2))).containsExactly(3L, 4L, 2L, 1L);
        for (var stage : requests.get(2).getStagesList())
            assertThat(stage.getBeforeCheckpoint()).isEqualTo(9);
    }

    @Test
    void allIdleTransitionPreservesBothAncestorWatermarksAndTheirNativeOutputOrder() throws Exception {
        var events = new ArrayList<String>();
        var scheduler = scheduler(
                request -> {
                    var stages = NativeControlInvocation.parseFrom(request).getStagesList();
                    for (var stage : stages)
                        if (stage.getPlanNodeId() == 1) events.add("native-root:" + stage.getWatermarkMillis());
                },
                events);
        scheduler.watermark(0, 200);
        scheduler.watermark(1, 100);
        scheduler.watermark(2, 300);
        scheduler.status(0, WatermarkStatus.IDLE);
        events.clear();
        scheduler.status(1, WatermarkStatus.IDLE);
        assertThat(events)
                .containsExactly("native-root:200", "root-watermark:200", "native-root:300", "root-watermark:300");
    }

    @Test
    void endInputCompletesOnlyItsPhysicalAncestorsAndFinishIsIdempotent() throws Exception {
        var requests = new ArrayList<NativeControlInvocation>();
        var scheduler =
                scheduler(request -> requests.add(NativeControlInvocation.parseFrom(request)), new ArrayList<>());
        scheduler.endInput(0);
        assertThat(requests).isEmpty();
        scheduler.endInput(1);
        assertThat(ids(requests.get(0))).containsExactly(3L);
        scheduler.endInput(1);
        assertThat(requests).hasSize(1);
        scheduler.endInput(2);
        assertThat(ids(requests.get(1))).containsExactly(4L, 2L, 1L);
        for (var request : requests)
            for (var stage : request.getStagesList())
                assertThat(stage.hasEndInput()).isTrue();
        scheduler.finish();
        assertThat(requests).hasSize(2);

        requests.clear();
        var finishing =
                scheduler(request -> requests.add(NativeControlInvocation.parseFrom(request)), new ArrayList<>());
        finishing.finish();
        assertThat(requests).hasSize(1);
        assertThat(ids(requests.get(0))).containsExactly(3L, 4L, 2L, 1L);
    }

    @Test
    void failedNativeDrainDoesNotForwardControlsAndCannotResumeAdvancedTree() throws Exception {
        var events = new ArrayList<String>();
        var failure = new IllegalStateException("drain failed");
        var scheduler = scheduler(
                request -> {
                    throw failure;
                },
                events);
        scheduler.watermark(0, 100);
        assertThatThrownBy(() -> scheduler.watermark(1, 100)).isSameAs(failure);
        assertThat(events).isEmpty();
        assertThatThrownBy(() -> scheduler.watermark(2, 100)).hasMessageContaining("requires recovery");
        assertThatThrownBy(scheduler::requireHealthy).hasMessageContaining("requires recovery");
        assertThatThrownBy(() -> scheduler.beforeCheckpoint(3)).hasMessageContaining("requires recovery");
    }

    @Test
    void malformedCapabilitiesCannotSilentlySkipAStage() {
        for (var capabilities : List.of(
                NativeControlCapabilities.newBuilder().setProtocolVersion(99).build(),
                NativeControlCapabilities.newBuilder()
                        .setProtocolVersion(1)
                        .addStages(capability(900))
                        .build(),
                NativeControlCapabilities.newBuilder()
                        .setProtocolVersion(1)
                        .addStages(capability(1))
                        .addStages(capability(1))
                        .build())) {
            assertThatThrownBy(() -> new NativeRegionControlScheduler(
                            NativeRegionControlTreeTest.plan(),
                            3,
                            capabilities.toByteArray(),
                            request -> {},
                            listener(new ArrayList<>())))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static List<Long> ids(NativeControlInvocation request) {
        assertThat(request.getProtocolVersion()).isEqualTo(1);
        return request.getStagesList().stream()
                .map(NativeStageControl::getPlanNodeId)
                .collect(java.util.stream.Collectors.toList());
    }

    private static NativeRegionControlScheduler scheduler(
            NativeRegionControlScheduler.Invocation invocation, List<String> events) {
        var capabilities = NativeControlCapabilities.newBuilder().setProtocolVersion(1);
        for (long id : List.of(3L, 4L, 2L, 1L)) capabilities.addStages(capability(id));
        return new NativeRegionControlScheduler(
                NativeRegionControlTreeTest.plan(),
                3,
                capabilities.build().toByteArray(),
                invocation,
                listener(events));
    }

    private static NativeStageControlCapability capability(long id) {
        return NativeStageControlCapability.newBuilder()
                .setPlanNodeId(id)
                .setWatermark(true)
                .setBeforeCheckpoint(true)
                .setEndInput(true)
                .build();
    }

    private static NativeRegionControlTree.Listener listener(List<String> events) {
        return new NativeRegionControlTree.Listener() {
            public void watermark(long id, long timestamp) {
                if (id == 1) events.add("root-watermark:" + timestamp);
            }

            public void status(long id, WatermarkStatus status) {
                if (id == 1) events.add("root-status:" + status.isIdle());
            }

            public void latency(long id, LatencyMarker marker) {}
        };
    }
}
