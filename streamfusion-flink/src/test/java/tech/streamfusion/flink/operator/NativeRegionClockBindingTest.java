/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativeControlCapabilities;
import tech.streamfusion.proto.plan.v1.NativeStageControlCapability;

class NativeRegionClockBindingTest {
    @Test
    void onlyVersionThreeNegotiatesDistinctClockPortsInDescriptorOrder() {
        var valid = capabilities(3, 2, 0, true);
        assertThat(scheduler(valid).processingTimeInputPorts()).containsExactly(0, 2);
        for (var invalid : new NativeControlCapabilities[] {
            capabilities(1, 0, 1, true), capabilities(2, 0, 1, true),
            capabilities(3, 0, 0, true), capabilities(3, -1, 1, true),
            capabilities(3, 3, 1, true), capabilities(3, 0, 1, false),
            capabilities(4, 0, 1, true)
        }) {
            assertThatThrownBy(() -> scheduler(invalid)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static NativeControlCapabilities capabilities(int version, int first, int second, boolean timer) {
        return NativeControlCapabilities.newBuilder()
                .setProtocolVersion(version)
                .addStages(NativeStageControlCapability.newBuilder()
                        .setPlanNodeId(1)
                        .setProcessingTime(timer)
                        .setProcessingTimeInputPort(first))
                .addStages(NativeStageControlCapability.newBuilder()
                        .setPlanNodeId(2)
                        .setProcessingTime(timer)
                        .setProcessingTimeInputPort(second))
                .build();
    }

    private static NativeRegionControlScheduler scheduler(NativeControlCapabilities capabilities) {
        return new NativeRegionControlScheduler(
                NativeRegionControlTreeTest.plan(),
                3,
                capabilities.toByteArray(),
                request -> {
                    throw new AssertionError("Unexpected invocation");
                },
                new NativeRegionControlTree.Listener() {
                    public void watermark(long id, long value) {}

                    public void status(long id, WatermarkStatus value) {}

                    public void latency(long id, LatencyMarker value) {}
                });
    }
}
