/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Random;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

class UnionRegionControlTest {
    @Test
    void nestedUnionChannelsMatchFlinksValveAcrossGeneratedIdleAndReactivationEvents() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            var expected = new CollectingDataOutput<Void>();
            var valve = new StatusWatermarkValve(3);
            var actual = new ArrayList<Object>();
            var tree = new NativeRegionControlTree(plan(), 3, new NativeRegionControlTree.Listener() {
                @Override
                public void watermark(long id, long timestamp) {
                    if (id == 1) actual.add(new Watermark(timestamp));
                }

                @Override
                public void status(long id, WatermarkStatus status) {
                    if (id == 1) actual.add(status);
                }

                @Override
                public void latency(long id, LatencyMarker marker) {}
            });
            var random = new Random(seed);
            boolean[] idle = new boolean[3];
            long[] times = new long[3];
            for (int step = 0; step < 160; step++) {
                int port = random.nextInt(3);
                if (step % 3 == 0) {
                    idle[port] = !idle[port];
                    var status = idle[port] ? WatermarkStatus.IDLE : WatermarkStatus.ACTIVE;
                    valve.inputWatermarkStatus(status, port, expected);
                    tree.status(port, status);
                } else if (!idle[port]) {
                    times[port] += random.nextInt(20) + 1;
                    valve.inputWatermark(new Watermark(times[port]), port, expected);
                    tree.watermark(port, times[port]);
                }
                assertThat(actual).as("seed %s step %s", seed, step).containsExactlyElementsOf(expected.getEvents());
            }
        }
    }

    private static byte[] plan() {
        Operator nested = Operator.newBuilder()
                .setPlanNodeId(3)
                .setUnion(Union.newBuilder().addInputs(edge(0)).addInputs(edge(1)))
                .build();
        Operator union = Operator.newBuilder()
                .setPlanNodeId(2)
                .setUnion(Union.newBuilder().addInputs(nested).addInputs(edge(2)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setCalc(Calc.newBuilder().setInput(union)))
                .build()
                .toByteArray();
    }

    private static Operator edge(int index) {
        return Operator.newBuilder()
                .setPlanNodeId(10 + index)
                .setInput(Input.newBuilder().setInputIndex(index))
                .build();
    }
}
