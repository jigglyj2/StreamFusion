/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.ExchangeDistribution;

/** Binds Flink's existing routing and state-backend lifecycle to one generic native region. */
final class NativeKeyedRegionTranslation {
    private NativeKeyedRegionTranslation() {}

    static Transformation<RowData> translate(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            StreamExecutionEnvironment environment) {
        return translate(inputs, inputTypes, outputType, plan, stateIds, environment, null);
    }

    static Transformation<RowData> translate(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            StreamExecutionEnvironment environment,
            java.util.function.Function<
                            List<Transformation<?>>,
                            java.util.Map<Long, tech.streamfusion.flink.memory.FlinkOperatorMemoryShare>>
                    resolver) {
        return translate(inputs, inputTypes, stateIds, environment, exchanges -> {
            var factory = new StreamFusionNativeRegionOperatorFactory(
                    inputTypes,
                    outputType,
                    plan,
                    stateIds,
                    exchanges,
                    resolver == null
                            ? tech.streamfusion.flink.window.NativeLocalWindowResources.NONE
                            : tech.streamfusion.flink.window.NativeLocalWindowResources.pending(plan));
            return resolver == null ? factory : factory.withResourceResolver(resolver);
        });
    }

    static Transformation<RowData> translate(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            List<Long> stateIds,
            StreamExecutionEnvironment environment,
            java.util.function.Function<List<byte[]>, StreamFusionNativeRegionOperatorFactory> factoryBuilder) {
        if (inputs.isEmpty() || inputs.size() != inputTypes.size() || stateIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "A keyed native region requires matching inputs and state-node identities");
        }
        if (stateIds.stream().anyMatch(id -> id <= 0)
                || stateIds.stream().distinct().count() != stateIds.size()) {
            throw new IllegalArgumentException("Native region state-node identities must be positive and unique");
        }
        var bindings = new ArrayList<NativeRegionInput>();
        for (int index = 0; index < inputs.size(); index++) {
            bindings.add(NativeRegionInput.bind(inputs.get(index), inputTypes.get(index), true));
        }
        var first = bindings.get(0).contract();
        boolean singleton = first.getDistribution() == ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON;
        if (!singleton && first.getDistribution() != ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH) {
            throw new IllegalArgumentException("Native keyed regions require hash or singleton distribution");
        }
        int maxParallelism =
                singleton ? KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM : first.getMaxParallelism();
        int parallelism = singleton ? 1 : first.getParallelism();
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        if (parallelism <= 0 || parallelism > maxParallelism) {
            throw new IllegalArgumentException("Invalid native keyed-region parallelism");
        }
        for (NativeRegionInput binding : bindings) {
            var contract = binding.contract();
            if (contract.getDistribution() != first.getDistribution()
                    || contract.getMaxParallelism() != first.getMaxParallelism()
                    || contract.getParallelism() != first.getParallelism()) {
                throw new IllegalArgumentException(
                        "Native keyed-region inputs must share a Flink key-group routing domain");
            }
        }
        StreamFusionStateBackendFactory.install(environment);
        var factory = factoryBuilder.apply(
                bindings.stream().map(binding -> binding.exchangePlan).collect(Collectors.toList()));
        var result = new KeyedMultipleInputTransformation<>(
                "streamfusion-native-region[inputs=" + inputs.size() + ",state=" + stateIds.size() + "]",
                factory,
                ArrowRowDataBatchTypeInfo.INSTANCE,
                parallelism,
                false,
                Types.INT);
        result.setMaxParallelism(maxParallelism);
        // Sharing one execution context must not collapse several persistent owners' weights
        // into a single owner's allowance. This is a relative Flink weight, not a new budget.
        result.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR,
                Math.multiplyExact(StreamFusionTaskMemory.STATEFUL_MANAGED_MEMORY_WEIGHT, stateIds.size()));
        var selector = new NativeExchangeFrameKeySelector(maxParallelism);
        for (NativeRegionInput binding : bindings) result.addInput(binding.frames, selector);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }
}
