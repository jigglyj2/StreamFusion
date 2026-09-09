/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
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
import tech.streamfusion.proto.plan.v1.NativeStageControlCapability;

class NativeRegionProcessingTimeControlTest {
    @Test
    void processingTimeTargetsOneOwnerWithoutAdvancingWatermarks() throws Exception {
        var requests = new ArrayList<NativeControlInvocation>();
        var watermarks = new ArrayList<Long>();
        var scheduler =
                scheduler(2, true, request -> requests.add(NativeControlInvocation.parseFrom(request)), watermarks);
        for (long timestamp : new long[] {Long.MIN_VALUE, -1, 0, 9999, Long.MAX_VALUE}) {
            scheduler.processingTime(1, timestamp);
            var request = requests.get(requests.size() - 1);
            assertThat(request.getProtocolVersion()).isEqualTo(2);
            assertThat(request.getStagesCount()).isEqualTo(1);
            assertThat(request.getStages(0).getPlanNodeId()).isEqualTo(1);
            assertThat(request.getStages(0).getProcessingTimeMillis()).isEqualTo(timestamp);
            assertThat(request.getStages(0).hasWatermarkMillis()).isFalse();
        }
        assertThat(watermarks).isEmpty();
        scheduler.watermark(0, 100);
        scheduler.watermark(1, 100);
        scheduler.watermark(2, 100);
        assertThat(watermarks).containsExactly(100L);
        assertThat(requests).hasSize(5);
        scheduler.beforeCheckpoint(7);
        assertThat(requests.get(5).getProtocolVersion()).isEqualTo(1);
        assertThat(requests.get(5).getStages(0).getBeforeCheckpoint()).isEqualTo(7);
    }

    @Test
    void unsupportedAndMalformedCapabilitiesFailBeforeInvokingNative() {
        assertThatThrownBy(() -> scheduler(1, true, request -> {}, new ArrayList<>()))
                .hasMessageContaining("require protocol 2");
        for (long id : List.of(1L, 99L)) {
            var scheduler = scheduler(
                    1,
                    false,
                    request -> {
                        throw new AssertionError("Unexpected native invocation");
                    },
                    new ArrayList<>());
            assertThatThrownBy(() -> scheduler.processingTime(id, 9999))
                    .hasMessageContaining("no processing-time timer capability");
        }
    }

    @Test
    void failedTimerOutputRequiresRecoveryBeforeFurtherControls() {
        var failure = new IllegalStateException("timer output failed");
        var scheduler = scheduler(
                2,
                true,
                request -> {
                    throw failure;
                },
                new ArrayList<>());
        assertThatThrownBy(() -> scheduler.processingTime(1, 9999)).isSameAs(failure);
        assertThatThrownBy(() -> scheduler.watermark(0, 20000)).hasMessageContaining("requires recovery");
        assertThatThrownBy(() -> scheduler.beforeCheckpoint(9)).hasMessageContaining("requires recovery");
    }

    private static NativeRegionControlScheduler scheduler(
            int version,
            boolean processingTime,
            NativeRegionControlScheduler.Invocation invocation,
            List<Long> watermarks) {
        byte[] capabilities = NativeControlCapabilities.newBuilder()
                .setProtocolVersion(version)
                .addStages(NativeStageControlCapability.newBuilder()
                        .setPlanNodeId(1)
                        .setBeforeCheckpoint(true)
                        .setProcessingTime(processingTime))
                .build()
                .toByteArray();
        return new NativeRegionControlScheduler(
                NativeRegionControlTreeTest.plan(),
                3,
                capabilities,
                invocation,
                new NativeRegionControlTree.Listener() {
                    public void watermark(long id, long value) {
                        if (id == 1) watermarks.add(value);
                    }

                    public void status(long id, WatermarkStatus status) {}

                    public void latency(long id, LatencyMarker marker) {}
                });
    }
}
