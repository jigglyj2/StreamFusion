/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.*;

class NativeRegionControlDagTest {
    @Test
    void generatedSharedControlWavesMatchFlinkAtEachStageAndOutput() throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var actual = new ArrayList<String>();
            var expected = new ArrayList<String>();
            var controls = new NativeRegionControlTree(plan(), Map.of(), listener(actual));
            assertThat(controls.outputIds()).containsExactly(11L, 13L, 14L);
            assertThatThrownBy(controls::rootId).hasMessageContaining("explicit output");
            // Flink's BroadcastingOutputCollector samples one branch for latency, while
            // watermarks/status are broadcast. Do not approximate that routing as broadcast.
            assertThatThrownBy(() -> controls.latency(0, new LatencyMarker(1, new OperatorID(), 0)))
                    .hasMessageContaining("sampled latency routing");
            try (var shared = new FlinkControlHarness(2);
                    var left = new FlinkControlHarness(1);
                    var right = new FlinkControlHarness(1);
                    var tail = new FlinkControlHarness(1)) {
                for (var harness : List.of(shared, left, right, tail)) {
                    harness.setup(StringSerializer.INSTANCE);
                    harness.open();
                }
                var random = new Random(seed);
                long[] times = {0, 0};
                for (int step = 0; step < 150; step++) {
                    int port = random.nextInt(2);
                    if (step % 4 < 2) {
                        times[port] += random.nextInt(40);
                        controls.watermark(port, times[port]);
                        shared.processWatermark(port, new Watermark(times[port]));
                    } else if (step % 4 == 2) {
                        var status = random.nextBoolean() ? WatermarkStatus.IDLE : WatermarkStatus.ACTIVE;
                        controls.status(port, status);
                        shared.processWatermarkStatus(port, status);
                    } else {
                        times[port] += random.nextInt(40);
                        controls.watermark(port, times[port]);
                        shared.processWatermark(port, new Watermark(times[port]));
                    }
                    drain(shared, 11, List.of(left, right), expected);
                    drain(left, 12, List.of(tail), expected);
                    drain(right, 13, List.of(), expected);
                    drain(tail, 14, List.of(), expected);
                    for (long id : List.of(11L, 12L, 13L, 14L)) {
                        assertThat(events(actual, id))
                                .as("seed=%s step=%s id=%s", seed, step, id)
                                .containsExactlyElementsOf(events(expected, id));
                    }
                }
            }
        }
    }

    @Test
    void oneInvocationDrainsSharedControlsBeforeAnyOutputFrontierAndFailureForwardsNothing() throws Exception {
        var capabilities = NativeControlCapabilities.newBuilder().setProtocolVersion(1);
        for (long id : List.of(11L, 12L, 13L, 14L))
            capabilities.addStages(NativeStageControlCapability.newBuilder()
                    .setPlanNodeId(id)
                    .setWatermark(true)
                    .setEndInput(true)
                    .setBeforeCheckpoint(true));
        var requests = new ArrayList<NativeControlInvocation>();
        var events = new ArrayList<String>();
        var scheduler = new NativeRegionControlScheduler(
                plan(),
                capabilities.build().toByteArray(),
                Map.of(),
                bytes -> {
                    requests.add(NativeControlInvocation.parseFrom(bytes));
                    events.add("native-output");
                },
                listener(events));
        scheduler.watermark(0, 100);
        assertThat(requests).isEmpty();
        scheduler.watermark(1, 200);
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).getStagesList())
                .extracting(NativeStageControl::getPlanNodeId)
                .containsExactly(11L, 12L, 14L, 13L);
        assertThat(events).containsExactly("native-output", "11:wm:100", "12:wm:100", "14:wm:100", "13:wm:100");
        scheduler.beforeCheckpoint(7);
        assertThat(requests.get(1).getStagesList())
                .extracting(NativeStageControl::getBeforeCheckpoint)
                .containsExactly(7L, 7L, 7L, 7L);
        scheduler.endInput(0);
        assertThat(requests).hasSize(2);
        scheduler.endInput(1);
        assertThat(requests.get(2).getStagesList()).allMatch(NativeStageControl::hasEndInput);
        scheduler.finish();
        assertThat(requests).hasSize(3);
        events.clear();
        var failed = new NativeRegionControlScheduler(
                plan(),
                capabilities.build().toByteArray(),
                Map.of(),
                bytes -> {
                    throw new IllegalStateException("drain failed");
                },
                listener(events));
        failed.watermark(0, 100);
        assertThatThrownBy(() -> failed.watermark(1, 200)).hasMessageContaining("drain failed");
        assertThat(events).isEmpty();
        assertThatThrownBy(() -> failed.beforeCheckpoint(8)).hasMessageContaining("requires recovery");
    }

    @Test
    void restoredSharedWindowClockReachesEveryExitOnceAndNestedUnionRequiresFlattening() throws Exception {
        var plan = plan().toBuilder().setInputCount(1);
        plan.setStages(
                0,
                plan.getStages(0).toBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(11)
                                .setWindowAggregate(WindowAggregate.newBuilder().setInput(input(0))))
                        .clearInputs()
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0)));
        var events = new ArrayList<String>();
        var controls = new NativeRegionControlTree(plan.build(), Map.of(11L, 500L), listener(events));
        controls.watermark(0, 100);
        assertThat(events).containsExactly("11:wm:500", "12:wm:500", "14:wm:500", "13:wm:500");
        assertThatThrownBy(() -> new NativeRegionControlTree(plan.build(), Map.of(12L, 500L), listener(events)))
                .hasMessageContaining("WindowAggregate");
        var union = Operator.newBuilder().setUnion(Union.newBuilder().addInputs(input(0)));
        plan.setStages(0, plan.getStages(0).toBuilder().setOperator(union.setPlanNodeId(11)));
        plan.setStages(1, plan.getStages(1).toBuilder().setOperator(union.setPlanNodeId(12)));
        assertThatThrownBy(() -> new NativeRegionControlTree(plan.build(), Map.of(), listener(events)))
                .hasMessageContaining("flattened UNION");
    }

    private static List<String> events(List<String> values, long id) {
        return values.stream()
                .filter(value -> value.startsWith(id + ":"))
                .collect(java.util.stream.Collectors.toList());
    }

    private static NativeRegionControlTree.Listener listener(List<String> values) {
        return new NativeRegionControlTree.Listener() {
            public void watermark(long id, long timestamp) {
                values.add(id + ":wm:" + timestamp);
            }

            public void status(long id, WatermarkStatus status) {
                values.add(id + ":idle:" + status.isIdle());
            }

            public void latency(long id, LatencyMarker marker) {
                values.add(id + ":latency:" + marker.getMarkedTime());
            }
        };
    }

    private static void drain(
            FlinkControlHarness from, long id, List<FlinkControlHarness> consumers, List<String> events)
            throws Exception {
        Object event;
        while ((event = from.getOutput().poll()) != null) {
            if (event instanceof Watermark) {
                events.add(id + ":wm:" + ((Watermark) event).getTimestamp());
                for (var consumer : consumers) consumer.processWatermark(0, (Watermark) event);
            } else if (event instanceof WatermarkStatus) {
                events.add(id + ":idle:" + ((WatermarkStatus) event).isIdle());
                for (var consumer : consumers) consumer.processWatermarkStatus(0, (WatermarkStatus) event);
            } else if (event instanceof LatencyMarker) {
                events.add(id + ":latency:" + ((LatencyMarker) event).getMarkedTime());
                for (var consumer : consumers) consumer.input(0).processLatencyMarker((LatencyMarker) event);
            }
        }
    }

    private static Operator input(int port) {
        return Operator.newBuilder()
                .setInput(Input.newBuilder().setInputIndex(port))
                .build();
    }

    private static NativeRegionPlan plan() {
        var plan = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(2)
                .addAllOutputStageIds(List.of(11L, 13L, 14L));
        plan.addStages(NativeRegionStage.newBuilder()
                .setOperator(Operator.newBuilder()
                        .setPlanNodeId(11)
                        .setRegularJoin(
                                RegularJoin.newBuilder().setLeftInput(input(0)).setRightInput(input(1))))
                .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0))
                .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(1)));
        for (int id = 12; id <= 14; id++)
            plan.addStages(NativeRegionStage.newBuilder()
                    .setOperator(Operator.newBuilder()
                            .setPlanNodeId(id)
                            .setCalc(Calc.newBuilder().setInput(input(0))))
                    .addInputs(NativeRegionInputReference.newBuilder().setStageId(id == 14 ? 12 : 11)));
        return plan.build();
    }
}
