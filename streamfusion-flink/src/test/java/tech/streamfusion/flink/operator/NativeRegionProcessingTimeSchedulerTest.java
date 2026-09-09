/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativeControlCapabilities;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;
import tech.streamfusion.proto.plan.v1.NativeStageControlCapability;

class NativeRegionProcessingTimeSchedulerTest {
    @Test
    void deadlinesUseFlinkClockAndCancelStaleCallbacksIncludingChangedTies() throws Exception {
        var f = new Fixture(true);
        f.pending.put(1L, 99L);
        f.pending.put(2L, 199L);
        f.driver.refresh();
        var stale = f.service.callbacks.get(0);
        f.pending.put(1L, 49L);
        f.driver.refresh();
        stale.onProcessingTime(99);
        assertThat(f.events).isEmpty();
        // Same timestamp, different owner: do not keep the old owner's cached callback.
        f.pending.put(1L, 199L);
        f.pending.put(2L, 49L);
        f.driver.refresh();
        assertThat(f.service.callbacks).hasSize(2);
        f.service.setCurrentTime(48);
        assertThat(f.events).isEmpty();
        f.service.setCurrentTime(49);
        assertThat(f.events).containsExactly("2:49");
        f.service.setCurrentTime(199);
        assertThat(f.events).containsExactly("2:49", "1:199");
        assertThat(f.service.getNumActiveTimers()).isZero();
        assertThat(f.watermarks).isEmpty();
    }

    @Test
    void restoredOverdueAndTiedDeadlinesFireAtRegisteredTimeAcrossSignedLongRange() throws Exception {
        for (long deadline : new long[] {Long.MIN_VALUE, -1, 0, 9999, Long.MAX_VALUE}) {
            var f = new Fixture(true);
            f.service.setCurrentTime(deadline == Long.MAX_VALUE ? deadline : deadline + 1);
            f.pending.put(1L, deadline);
            f.pending.put(2L, deadline);
            f.driver.refresh();
            assertThat(f.service.getNumActiveTimers()).isEqualTo(1);
            assertThat(f.events).isEmpty();
            f.service.setCurrentTime(f.service.getCurrentProcessingTime());
            assertThat(f.events).containsExactly("1:" + deadline, "2:" + deadline);
            assertThat(f.service.getNumActiveTimers()).isZero();
            assertThat(f.watermarks).isEmpty();
            f.driver.close();
        }
    }

    @Test
    void closeLeavesOpenWindowsUnfiredAndCannotRearmAfterFinalCheckpoint() throws Exception {
        var f = new Fixture(true);
        f.pending.put(1L, 9999L);
        f.driver.refresh();
        var stale = f.service.callbacks.get(0);
        f.driver.close();
        f.driver.close();
        f.controls.beforeCheckpoint(10);
        f.driver.refresh();
        f.service.setCurrentTime(Long.MAX_VALUE);
        stale.onProcessingTime(9999);
        assertThat(f.events).isEmpty();
        assertThat(f.pending).containsEntry(1L, 9999L);
        assertThat(f.service.getNumActiveTimers()).isZero();
    }

    @Test
    void legacyStagesNeverReadDeadlinesOrRegisterTimers() {
        var f = new Fixture(false);
        f.driver.refresh();
        f.driver.refresh();
        assertThat(f.reads).isZero();
        assertThat(f.service.callbacks).isEmpty();
    }

    @Test
    void malformedSnapshotsAndRegistrationFailurePoisonFurtherMutation() {
        for (long[] invalid : new long[][] {null, {1}, {99, 1}, {1, 2, 1, 3}, {1, 2, 2, 3, 3, 4}}) {
            var f = new Fixture(true);
            f.override = true;
            f.snapshot = invalid;
            assertThatThrownBy(f.driver::refresh).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(f.controls::requireHealthy).hasMessageContaining("requires recovery");
            assertThat(f.service.getNumActiveTimers()).isZero();
        }
        var f = new Fixture(true);
        f.service.shutdownService();
        f.pending.put(1L, 9999L);
        assertThatThrownBy(f.driver::refresh).hasMessageContaining("terminated");
        assertThatThrownBy(f.controls::requireHealthy).hasMessageContaining("requires recovery");
    }

    @Test
    void failedOrNonAdvancingTimerRequiresRecoveryAndDoesNotSpin() {
        for (boolean outputFailure : new boolean[] {false, true}) {
            var f = new Fixture(true);
            f.pending.put(1L, 99L);
            f.removeFired = false;
            f.fail = outputFailure;
            f.driver.refresh();
            assertThatThrownBy(() -> f.service.setCurrentTime(100))
                    .hasMessageContaining(outputFailure ? "output failed" : "did not advance");
            assertThatThrownBy(f.controls::requireHealthy).hasMessageContaining("requires recovery");
            assertThat(f.service.getNumActiveTimers()).isZero();
        }
    }

    private static final class RecordingService extends TestProcessingTimeService {
        private final List<ProcessingTimeCallback> callbacks = new ArrayList<>();

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
            callbacks.add(callback);
            return super.registerTimer(timestamp, callback);
        }
    }

    private static final class Fixture {
        final RecordingService service = new RecordingService();
        final Map<Long, Long> pending = new LinkedHashMap<>();
        final List<String> events = new ArrayList<>();
        final List<Long> watermarks = new ArrayList<>();
        final NativeRegionControlScheduler controls;
        final NativeRegionProcessingTimeScheduler driver;
        boolean removeFired = true;
        boolean fail;
        boolean override;
        long[] snapshot;
        int reads;

        Fixture(boolean supported) {
            var capabilities = NativeControlCapabilities.newBuilder().setProtocolVersion(supported ? 2 : 1);
            for (long id : List.of(1L, 2L)) {
                capabilities.addStages(NativeStageControlCapability.newBuilder()
                        .setPlanNodeId(id)
                        .setProcessingTime(supported));
            }
            controls = new NativeRegionControlScheduler(
                    NativeRegionControlTreeTest.plan(),
                    3,
                    capabilities.build().toByteArray(),
                    request -> {
                        if (fail) throw new IllegalStateException("output failed");
                        var invocation = NativeControlInvocation.parseFrom(request);
                        assertThat(invocation.getProtocolVersion()).isEqualTo(2);
                        for (var stage : invocation.getStagesList()) {
                            assertThat(stage.hasProcessingTimeMillis()).isTrue();
                            events.add(stage.getPlanNodeId() + ":" + stage.getProcessingTimeMillis());
                            if (removeFired) pending.remove(stage.getPlanNodeId());
                        }
                    },
                    new NativeRegionControlTree.Listener() {
                        public void watermark(long id, long time) {
                            watermarks.add(time);
                        }

                        public void status(long id, WatermarkStatus status) {}

                        public void latency(long id, LatencyMarker marker) {}
                    });
            driver = new NativeRegionProcessingTimeScheduler(controls, service, () -> {
                reads++;
                if (override) return snapshot;
                return pending.entrySet().stream()
                        .flatMapToLong(entry -> java.util.stream.LongStream.of(entry.getKey(), entry.getValue()))
                        .toArray();
            });
        }
    }
}
