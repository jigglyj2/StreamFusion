/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.metrics.StreamFusionNativeMetricTree;

/** Real Flink control operators and Flink's input/output gauge definitions are the oracle. */
class NativeRegionWatermarkMetricsTest {
    @Test
    void generatedInputAndOutputGaugesMatchThroughIdleReactivationAtEveryVirtualStage() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            var groups = new HashMap<Long, InterceptingOperatorMetricGroup>();
            var task = new InterceptingTaskMetricGroup() {
                @Override
                public InternalOperatorMetricGroup getOrAddOperator(
                        OperatorID id, String name, Map<String, String> variables) {
                    var group = new InterceptingOperatorMetricGroup();
                    groups.put(ByteBuffer.wrap(id.getBytes()).getLong(8), group);
                    return group;
                }
            };
            // The runtime boundary is separate, so even the output/root SQL stage is observable.
            try (var metrics = StreamFusionNativeMetricTree.forRegion(
                            NativeRegionControlTreeTest.plan(), new OperatorID(0, 0), task, new Configuration(), 0);
                    var inner = new Reference(2);
                    var right = new Reference(1);
                    var outer = new Reference(2);
                    var root = new Reference(1)) {
                var controls = new NativeRegionControlTree(
                        NativeRegionControlTreeTest.plan(), 3, new NativeRegionControlTree.Listener() {
                            @Override
                            public void inputWatermark(long id, int port, long timestamp) {
                                metrics.inputWatermark(id, port, timestamp);
                            }

                            @Override
                            public void watermark(long id, long timestamp) {
                                metrics.watermark(id, timestamp);
                            }

                            @Override
                            public void status(long id, WatermarkStatus status) {}

                            @Override
                            public void latency(long id, LatencyMarker marker) {}
                        });
                Map<Long, Reference> references = Map.of(1L, root, 2L, outer, 3L, inner, 4L, right);
                var random = new Random(seed);
                long[] times = {0, 0, 0};
                boolean[] idle = new boolean[3];
                boolean observedDifferentInputAndOutput = false;
                for (int step = 0; step < 160; step++) {
                    int port = random.nextInt(3);
                    Reference entry = port == 2 ? right : inner;
                    int input = port == 2 ? 0 : port;
                    if (step % 3 == 0) {
                        idle[port] = !idle[port];
                        var status = idle[port] ? WatermarkStatus.IDLE : WatermarkStatus.ACTIVE;
                        controls.status(port, status);
                        entry.harness.processWatermarkStatus(input, status);
                    } else if (!idle[port]) {
                        times[port] += 1 + random.nextInt(40);
                        controls.watermark(port, times[port]);
                        entry.watermark(input, new Watermark(times[port]));
                    }
                    inner.drain(outer, 0);
                    right.drain(outer, 1);
                    outer.drain(root, 0);
                    root.drain(null, 0);
                    for (var stage : references.entrySet()) {
                        var actual = groups.get(stage.getKey());
                        assertThat(actual.get("currentInputWatermark")).isInstanceOf(Gauge.class);
                        assertThat(((Gauge<?>) actual.get("currentInputWatermark")).getValue())
                                .as("input seed=%s step=%s stage=%s", seed, step, stage.getKey())
                                .isEqualTo(stage.getValue().minimum.getValue());
                        assertThat(((Gauge<?>) actual.get("currentOutputWatermark")).getValue())
                                .as("output seed=%s step=%s stage=%s", seed, step, stage.getKey())
                                .isEqualTo(stage.getValue().output.getValue());
                        observedDifferentInputAndOutput |= !stage.getValue()
                                .minimum
                                .getValue()
                                .equals(stage.getValue().output.getValue());
                    }
                }
                assertThat(observedDifferentInputAndOutput)
                        .as("idle-aware output must diverge from input gauge")
                        .isTrue();
            }
        }
    }

    private static final class Reference implements AutoCloseable {
        final FlinkControlHarness harness;
        final WatermarkGauge[] inputs;
        final MinWatermarkGauge minimum;
        final WatermarkGauge output = new WatermarkGauge();

        Reference(int arity) throws Exception {
            harness = new FlinkControlHarness(arity);
            harness.setup(StringSerializer.INSTANCE);
            harness.open();
            inputs = new WatermarkGauge[arity];
            java.util.Arrays.setAll(inputs, ignored -> new WatermarkGauge());
            minimum = new MinWatermarkGauge(inputs);
        }

        void watermark(int port, Watermark mark) throws Exception {
            // StreamTask's network output updates the gauge before invoking the operator.
            inputs[port].setCurrentWatermark(mark.getTimestamp());
            harness.processWatermark(port, mark);
        }

        void drain(Reference next, int port) throws Exception {
            Object event;
            while ((event = harness.getOutput().poll()) != null) {
                if (event instanceof Watermark) {
                    var mark = (Watermark) event;
                    output.setCurrentWatermark(mark.getTimestamp());
                    if (next != null) next.watermark(port, mark);
                } else if (event instanceof WatermarkStatus && next != null) {
                    next.harness.processWatermarkStatus(port, (WatermarkStatus) event);
                }
            }
        }

        @Override
        public void close() throws Exception {
            harness.close();
        }
    }
}
