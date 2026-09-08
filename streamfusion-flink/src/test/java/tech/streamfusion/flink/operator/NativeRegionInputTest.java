/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;

class NativeRegionInputTest {
    private static final RowType TYPE = RowType.of(new IntType(false));

    @Test
    void fusionPreservesSummedStateOwnerWeightIndependentlyOfInputArity() {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        for (int arity : List.of(1, 2))
            for (int owners : List.of(1, 2, 5)) {
                var inputs = java.util.stream.IntStream.range(0, arity)
                        .mapToObj(ignored -> exchange(16, 2))
                        .collect(java.util.stream.Collectors.toList());
                var types = java.util.Collections.nCopies(arity, TYPE);
                var ids = java.util.stream.LongStream.rangeClosed(1, owners)
                        .boxed()
                        .collect(java.util.stream.Collectors.toList());
                var combined = StreamFusionNativeRegionTranslator.translateKeyedInputs(
                        inputs, types, TYPE, StreamFusionNativeRegionTranslator.inputPlan(0), ids, environment);
                var useCase = org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR;
                int independentWeight = 0;
                for (long id : ids) {
                    var independent = StreamFusionNativeRegionTranslator.translateKeyedInputs(
                            inputs,
                            types,
                            TYPE,
                            StreamFusionNativeRegionTranslator.inputPlan(0),
                            List.of(id),
                            environment);
                    assertThat(independent.getManagedMemoryOperatorScopeUseCaseWeights())
                            .containsEntry(useCase, 8);
                    independentWeight += independent
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .get(useCase);
                }
                assertThat(combined.getManagedMemoryOperatorScopeUseCaseWeights())
                        .containsEntry(useCase, independentWeight);
                assertThat(combined.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            }
    }

    @Test
    void keyedTranslationRetainsExchangeFramesContractsAndFlinkKeyGroups() throws Exception {
        var first = exchange(16, 2);
        var second = exchange(16, 2);
        var region = (KeyedMultipleInputTransformation<?>) StreamFusionNativeRegionTranslator.translateKeyedInputs(
                List.of(first, second),
                List.of(TYPE, TYPE),
                TYPE,
                StreamFusionNativeRegionTranslator.inputPlan(0),
                List.of(10L, 20L),
                StreamExecutionEnvironment.getExecutionEnvironment());
        assertThat(region.getInputs())
                .containsExactly(first.getInputs().get(0), second.getInputs().get(0));
        assertThat(region.getMaxParallelism()).isEqualTo(16);
        assertThat(region.getParallelism()).isEqualTo(2);
        assertThat(region.getStateKeySelectors()).hasSize(2);
        assertThat(region.getManagedMemoryOperatorScopeUseCaseWeights())
                .containsEntry(org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR, 16);
        for (var selector : region.getStateKeySelectors()) {
            for (int group = 0; group < 16; group++) {
                int key = ((NativeExchangeFrameKeySelector) selector)
                        .getKey(new NativeExchangeFrame(group, new byte[0], new byte[0]));
                assertThat(KeyGroupRangeAssignment.assignToKeyGroup(key, 16)).isEqualTo(group);
            }
        }
        var plans = region.getOperatorFactory().getClass().getDeclaredField("exchangePlans");
        plans.setAccessible(true);
        assertThat((List<byte[]>) plans.get(region.getOperatorFactory()))
                .usingRecursiveComparison()
                .isEqualTo(List.of(
                        NativeRegionInput.bind(first, TYPE, true).exchangePlan,
                        NativeRegionInput.bind(second, TYPE, true).exchangePlan));
    }

    @Test
    void aSingleArrowSourceRemainsDirectWithoutAnIpcWriter() {
        var input = arrowSource();
        var region = StreamFusionNativeRegionTranslator.translateInputs(
                List.of(input), List.of(TYPE), TYPE, StreamFusionNativeRegionTranslator.inputPlan(0));
        assertThat(region).isInstanceOf(org.apache.flink.streaming.api.transformations.OneInputTransformation.class);
        assertThat(region.getInputs()).containsExactly(input);
        assertThat(region.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
    }

    @Test
    void statelessRegionAlsoDecodesAnExistingExchangeOnlyOnce() throws Exception {
        var input = exchange(16, 2);
        var region = (MultipleInputTransformation<?>) StreamFusionNativeRegionTranslator.translateInputs(
                List.of(input), List.of(TYPE), TYPE, StreamFusionNativeRegionTranslator.inputPlan(0));
        assertThat(region.getInputs()).containsExactly(input.getInputs().get(0));
        var planField = region.getOperatorFactory().getClass().getDeclaredField("plan");
        planField.setAccessible(true);
        var plan = tech.streamfusion.proto.plan.v1.NativePlan.parseFrom(
                (byte[]) planField.get(region.getOperatorFactory()));
        assertThat(plan.getProtocolVersion()).isEqualTo(3);
    }

    @Test
    void rejectsMissingRoutingAndIncompatibleDomainsWithoutInventingAnExchange() {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        for (var other : List.of(
                exchange(32, 2), exchange(16, 1), StreamFusionExchangeTranslator.singleton(arrowSource(), TYPE))) {
            assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.translateKeyedInputs(
                            List.of(exchange(16, 2), other),
                            List.of(TYPE, TYPE),
                            TYPE,
                            new byte[0],
                            List.of(1L),
                            environment))
                    .hasMessageContaining("routing domain");
        }
        assertThatThrownBy(() -> NativeRegionInput.bind(arrowSource(), TYPE, true))
                .hasMessageContaining("planned exchange");
    }

    private static Transformation<RowData> exchange(int groups, int parallelism) {
        return StreamFusionExchangeTranslator.hash(arrowSource(), TYPE, new int[] {0}, groups, parallelism, true);
    }

    static Transformation<RowData> arrowSource() {
        return StreamFusionArrowBoundaries.asPlannerTransformation(
                new Transformation<ArrowRowDataBatch>("test Arrow source", ArrowRowDataBatchTypeInfo.INSTANCE, 1) {
                    @Override
                    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                        return List.of(this);
                    }

                    @Override
                    public List<Transformation<?>> getInputs() {
                        return List.of();
                    }
                });
    }
}
