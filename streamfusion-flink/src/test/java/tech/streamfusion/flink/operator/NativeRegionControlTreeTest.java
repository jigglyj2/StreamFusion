/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class NativeRegionControlTreeTest {
    @Test
    void generatedControlEventsMatchRealFlinkOperatorsAtEveryStageIncludingIdleReactivation() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            var actual = new ArrayList<String>();
            var expected = new ArrayList<String>();
            var controls = new NativeRegionControlTree(plan(), 3, listener(actual));
            try (var inner = new FlinkControlHarness(2);
                    var right = new FlinkControlHarness(1);
                    var outer = new FlinkControlHarness(2);
                    var root = new FlinkControlHarness(1)) {
                for (FlinkControlHarness harness : List.of(inner, right, outer, root)) {
                    harness.setup(StringSerializer.INSTANCE);
                    harness.open();
                }
                var random = new Random(seed);
                long[] times = {0, 0, 0};
                for (int step = 0; step < 120; step++) {
                    int port = random.nextInt(3);
                    FlinkControlHarness entry = port == 2 ? right : inner;
                    int input = port == 2 ? 0 : port;
                    switch (step % 4) {
                        case 0:
                        case 1:
                            times[port] += random.nextInt(40);
                            controls.watermark(port, times[port]);
                            entry.processWatermark(input, new Watermark(times[port]));
                            break;
                        case 2:
                            WatermarkStatus status =
                                    random.nextBoolean() ? WatermarkStatus.IDLE : WatermarkStatus.ACTIVE;
                            controls.status(port, status);
                            entry.processWatermarkStatus(input, status);
                            break;
                        default:
                            var marker = new LatencyMarker(step, new OperatorID(), port);
                            controls.latency(port, marker);
                            entry.input(input).processLatencyMarker(marker);
                    }
                    drain(inner, 3, outer, 0, expected);
                    drain(right, 4, outer, 1, expected);
                    drain(outer, 2, root, 0, expected);
                    drain(root, 1, null, 0, expected);
                    for (int stage = 1; stage <= 4; stage++) {
                        String prefix = stage + ":";
                        assertThat(actual.stream()
                                        .filter(event -> event.startsWith(prefix))
                                        .collect(java.util.stream.Collectors.toList()))
                                .as("seed %s step %s stage %s", seed, step, stage)
                                .containsExactlyElementsOf(expected.stream()
                                        .filter(event -> event.startsWith(prefix))
                                        .collect(java.util.stream.Collectors.toList()));
                    }
                }
            }
        }
    }

    @Test
    void reactivationDoesNotFlattenAwayNestedWatermarkProgressAndInvalidBindingsFailClosed() throws Exception {
        var events = new ArrayList<String>();
        var controls = new NativeRegionControlTree(plan(), 3, listener(events));
        controls.watermark(0, 200);
        controls.status(1, WatermarkStatus.IDLE);
        controls.watermark(2, 50);
        controls.status(1, WatermarkStatus.ACTIVE);
        controls.watermark(2, 150);
        assertThat(controls.watermark()).isEqualTo(150);
        assertThat(events).contains("3:wm:200", "1:wm:150");
        assertThatThrownBy(() -> new NativeRegionControlTree(plan(), 4, listener(events)))
                .hasMessageContaining("unbound");
        assertThatThrownBy(() -> new NativeRegionControlTree(plan(), 2, listener(events)))
                .hasMessageContaining("in range");
        assertThatThrownBy(() -> controls.watermark(-1, 10)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    private static NativeRegionControlTree.Listener listener(List<String> events) {
        return new NativeRegionControlTree.Listener() {
            private void add(long id, String event) {
                if (id < 10) {
                    events.add(id + ":" + event);
                }
            }

            @Override
            public void watermark(long nodeId, long timestamp) {
                add(nodeId, "wm:" + timestamp);
            }

            @Override
            public void status(long nodeId, WatermarkStatus status) {
                add(nodeId, "idle:" + status.isIdle());
            }

            @Override
            public void latency(long nodeId, LatencyMarker marker) {
                add(nodeId, "latency:" + marker.getMarkedTime());
            }
        };
    }

    private static void drain(FlinkControlHarness from, long id, FlinkControlHarness to, int port, List<String> events)
            throws Exception {
        Object event;
        while ((event = from.getOutput().poll()) != null) {
            if (event instanceof Watermark) {
                var watermark = (Watermark) event;
                events.add(id + ":wm:" + watermark.getTimestamp());
                if (to != null) {
                    to.processWatermark(port, watermark);
                }
            } else if (event instanceof WatermarkStatus) {
                var status = (WatermarkStatus) event;
                events.add(id + ":idle:" + status.isIdle());
                if (to != null) {
                    to.processWatermarkStatus(port, status);
                }
            } else if (event instanceof LatencyMarker) {
                var marker = (LatencyMarker) event;
                events.add(id + ":latency:" + marker.getMarkedTime());
                if (to != null) {
                    to.input(port).processLatencyMarker(marker);
                }
            }
        }
    }

    static byte[] plan() {
        Operator inner = Operator.newBuilder()
                .setPlanNodeId(3)
                .setRegularJoin(tech.streamfusion.proto.plan.v1.RegularJoin.newBuilder()
                        .setLeftInput(edge(0))
                        .setRightInput(edge(1)))
                .build();
        Operator right = Operator.newBuilder()
                .setPlanNodeId(4)
                .setCalc(Calc.newBuilder().setInput(edge(2)))
                .build();
        Operator outer = Operator.newBuilder()
                .setPlanNodeId(2)
                .setRegularJoin(tech.streamfusion.proto.plan.v1.RegularJoin.newBuilder()
                        .setLeftInput(inner)
                        .setRightInput(right))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setCalc(Calc.newBuilder().setInput(outer)))
                .build()
                .toByteArray();
    }

    private static Operator edge(int index) {
        return Operator.newBuilder()
                .setPlanNodeId(10 + index)
                .setInput(tech.streamfusion.proto.plan.v1.Input.newBuilder().setInputIndex(index))
                .build();
    }
}
