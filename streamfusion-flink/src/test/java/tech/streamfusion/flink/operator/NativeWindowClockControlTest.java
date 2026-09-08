/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.*;

class NativeWindowClockControlTest {
    @Test
    void restoresEachWindowBeforeCombiningDownstreamWhileInputGaugesObserveReplay() throws Exception {
        var events = new ArrayList<String>();
        var requests = new ArrayList<NativeControlInvocation>();
        var capabilities = NativeControlCapabilities.newBuilder().setProtocolVersion(1);
        for (long id : List.of(3L, 4L))
            capabilities.addStages(
                    NativeStageControlCapability.newBuilder().setPlanNodeId(id).setWatermark(true));
        var scheduler = new NativeRegionControlScheduler(
                plan(),
                2,
                capabilities.build().toByteArray(),
                Map.of(3L, 100L, 4L, 300L),
                request -> {
                    requests.add(NativeControlInvocation.parseFrom(request));
                    events.add("native");
                },
                listener(events));
        scheduler.watermark(0, 50);
        assertThat(events).containsSubsequence("input:3:50", "native", "output:3:100");
        assertThat(events).doesNotContain("output:5:100");
        events.clear();
        scheduler.watermark(1, 70);
        assertThat(events).containsSubsequence("input:4:70", "native", "output:4:300", "output:5:100");
        assertThat(requests.get(0).getStages(0).getWatermarkMillis()).isEqualTo(100);
        assertThat(requests.get(1).getStages(0).getWatermarkMillis()).isEqualTo(300);
        events.clear();
        scheduler.watermark(0, 200);
        assertThat(events).containsSubsequence("native", "output:3:200", "output:5:200");
    }

    @Test
    void aRestoredClockCannotBeAttachedToAnAbsentOrNonWindowStage() {
        for (long id : List.of(1L, 5L, 99L)) {
            assertThatThrownBy(
                            () -> new NativeRegionControlTree(plan(), 2, Map.of(id, 100L), listener(new ArrayList<>())))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("clock");
        }
    }

    private static byte[] plan() {
        var left = window(3, 1, 0);
        var right = window(4, 2, 1);
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(5)
                        .setUnion(Union.newBuilder().addInputs(left).addInputs(right)))
                .build()
                .toByteArray();
    }

    private static Operator window(long id, long inputId, int port) {
        return Operator.newBuilder()
                .setPlanNodeId(id)
                .setWindowAggregate(WindowAggregate.newBuilder()
                        .setInput(Operator.newBuilder()
                                .setPlanNodeId(inputId)
                                .setInput(Input.newBuilder().setInputIndex(port))))
                .build();
    }

    private static NativeRegionControlTree.Listener listener(List<String> events) {
        return new NativeRegionControlTree.Listener() {
            public void inputWatermark(long id, int port, long timestamp) {
                events.add("input:" + id + ":" + timestamp);
            }

            public void watermark(long id, long timestamp) {
                events.add("output:" + id + ":" + timestamp);
            }

            public void status(long id, WatermarkStatus status) {}

            public void latency(long id, LatencyMarker marker) {}
        };
    }
}
