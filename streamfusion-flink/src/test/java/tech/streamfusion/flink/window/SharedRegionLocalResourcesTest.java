/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.window;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.proto.plan.v1.*;

class SharedRegionLocalResourcesTest {
    @Test
    void sharedLocalStageRetainsOneOriginalCapacityAcrossFactorySerialization() throws Exception {
        var plan = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(1)
                .addOutputStageIds(3)
                .addOutputStageIds(4)
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(3)
                                .setLocalWindowAggregate(LocalWindowAggregate.newBuilder()
                                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))))
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0)))
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder()
                                .setPlanNodeId(4)
                                .setCalc(Calc.newBuilder()
                                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))))
                        .addInputs(NativeRegionInputReference.newBuilder().setStageId(3)))
                .build();
        var share = new FlinkOperatorMemoryShare(1, 2, Set.of(ManagedMemoryUseCase.OPERATOR));
        var pending = NativeLocalWindowResources.pending(plan);
        assertThat(pending.isPending()).isTrue();
        assertThatThrownBy(() -> pending.resolvedFrom(Map.of())).hasMessageContaining("missing local-window share");
        var resources = InstantiationUtil.clone(pending.resolvedFrom(Map.of(3L, share)));
        resources.validate(plan);
        assertThatThrownBy(() -> NativeLocalWindowResources.NONE.validate(plan)).hasMessageContaining("match");
        assertThatThrownBy(() -> new NativeLocalWindowResources(Map.of(3L, share, 4L, share)).validate(plan))
                .hasMessageContaining("match");
        try (var environment =
                new MockEnvironmentBuilder().setManagedMemorySize(64L << 20).build()) {
            var runtime = new StreamConfig(new Configuration());
            runtime.setStateBackendUsesManagedMemory(false);
            runtime.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            var bindings = NativeTaskBindings.parseFrom(resources.resolve(environment, runtime));
            assertThat(bindings.getBindingsCount()).isOne();
            assertThat(bindings.getBindings(0).getPlanNodeId()).isEqualTo(3);
            assertThat(bindings.getBindings(0).getLocalWindowBuffer().getFlinkBufferMemoryBytes())
                    .isEqualTo(32L << 20);
            assertThat(bindings.getBindings(0).getLocalWindowBuffer().getFlinkPageBytes())
                    .isEqualTo(environment.getMemoryManager().getPageSize());
            assertThat(environment.getMemoryManager().verifyEmpty()).isTrue();
        }
    }
}
