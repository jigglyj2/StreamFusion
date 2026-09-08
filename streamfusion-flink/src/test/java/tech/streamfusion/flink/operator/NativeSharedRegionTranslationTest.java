/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;

class NativeSharedRegionTranslationTest {
    @Test
    void exitsAreVirtualArrowSelectionsOfOneOwnerAndDoNotAddAnExchange() throws Exception {
        var input = NativeRegionInputTest.arrowSource();
        var outputs = NativeSharedRegionTranslation.translate(
                List.of(input),
                List.of(SharedNativeRegionRuntimeTest.TYPE),
                List.of(SharedNativeRegionRuntimeTest.TYPE, SharedNativeRegionRuntimeTest.TEXT),
                SharedNativeRegionRuntimeTest.plan().toByteArray(),
                List.of(),
                StreamExecutionEnvironment.getExecutionEnvironment(),
                null);
        assertThat(outputs).hasSize(2).allSatisfy(output -> assertThat(output.getOutputType())
                .isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE));
        var owner = (OneInputTransformation<?, ?>) outputs.get(0);
        assertThat(owner.getInputs()).containsExactly(input);
        assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
        var side = (SideOutputTransformation<?>) outputs.get(1);
        assertThat(side.getInputs()).containsExactly(owner);
        assertThat(side.getOutputTag())
                .isEqualTo(((StreamFusionNativeRegionOperatorFactory) owner.getOperatorFactory()).outputTag(1));
        assertThat(side.getTransitivePredecessors()).containsExactly(side, owner, input);
    }

    @Test
    void rejectsEnabledLatencyTrackingBeforeBuildingAnOwner() throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setLatencyTrackingInterval(100);
        assertThatThrownBy(() -> NativeSharedRegionTranslation.translate(
                        List.of(NativeRegionInputTest.arrowSource()),
                        List.of(SharedNativeRegionRuntimeTest.TYPE),
                        List.of(SharedNativeRegionRuntimeTest.TYPE, SharedNativeRegionRuntimeTest.TEXT),
                        SharedNativeRegionRuntimeTest.plan().toByteArray(),
                        List.of(),
                        environment,
                        null))
                .hasMessageContaining("sampled latency routing");
    }
}
