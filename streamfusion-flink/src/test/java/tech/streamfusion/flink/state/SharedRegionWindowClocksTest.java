/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.junit.jupiter.api.Test;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.*;

class SharedRegionWindowClocksTest {
    @Test
    void oneUnionStateClockPerWindowRestoresTheMinimumAcrossRescaledSubtasks() throws Exception {
        var plan = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(1)
                .addOutputStageIds(11)
                .addOutputStageIds(12)
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(11)
                                .setWindowAggregate(WindowAggregate.newBuilder()
                                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))))
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0)))
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(12)
                                .setCalc(Calc.newBuilder()
                                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))))
                        .addInputs(NativeRegionInputReference.newBuilder().setStageId(11)))
                .build();
        for (List<Long> previous : List.of(List.<Long>of(), List.of(700L, 500L, 900L), List.of(Long.MIN_VALUE, 700L))) {
            var values = new ArrayList<>(previous);
            var requests = new ArrayList<String>();
            var store = (OperatorStateStore) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {OperatorStateStore.class}, (proxy, method, args) -> {
                        assertThat(method.getName()).isEqualTo("getUnionListState");
                        requests.add(((ListStateDescriptor<?>) args[0]).getName());
                        return Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class<?>[] {ListState.class}, (p, m, a) -> {
                                    if (m.getName().equals("get")) return List.copyOf(values);
                                    if (m.getName().equals("update")) {
                                        values.clear();
                                        for (Object value : (List<?>) a[0]) values.add((Long) value);
                                        return null;
                                    }
                                    throw new UnsupportedOperationException(m.toString());
                                });
                    });
            var initialization = (StateInitializationContext) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {StateInitializationContext.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("isRestored")) return !previous.isEmpty();
                        if (method.getName().equals("getOperatorStateStore")) return store;
                        throw new UnsupportedOperationException(method.toString());
                    });
            var clocks = new NativeRegionWindowClocks(initialization, plan, List.of(11L));
            assertThat(requests).containsExactly("streamfusion-window-watermark-v1-11");
            long minimum = previous.stream().mapToLong(Long::longValue).min().orElse(Long.MIN_VALUE);
            assertThat(clocks.restored()).isEqualTo(previous.isEmpty() ? Map.of() : Map.of(11L, minimum));
            var binding = clocks.bind(NativeStateResources.memory(11, 16, 0, 15));
            assertThat(binding.hasRestoredWatermark()).isEqualTo(!previous.isEmpty());
            if (!previous.isEmpty()) assertThat(binding.getRestoredWatermark()).isEqualTo(minimum);
            clocks.snapshot();
            assertThat(values).containsExactly(minimum);
            clocks.watermark(11, 1000);
            clocks.watermark(11, 500);
            clocks.watermark(12, 9999);
            clocks.snapshot();
            assertThat(values).containsExactly(1000L);
        }
    }
}
