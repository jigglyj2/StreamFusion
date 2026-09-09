/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;

/** One physical Flink owner with virtual Arrow exits; no additional computation or exchange. */
public final class NativeSharedRegionTranslation {
    private NativeSharedRegionTranslation() {}

    public static void validate(byte[] bytes, int outputCount) {
        NativeSharedRegionOutputs.validate(decode(bytes), outputCount);
        if (tech.streamfusion.nativebridge.NativeRegionStream.edgeVersion() != 2)
            throw new IllegalArgumentException("Shared native regions require Arrow/IPC region edge version 2");
    }

    public static List<Transformation<RowData>> translate(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            List<RowType> outputTypes,
            byte[] bytes,
            List<Long> stateIds,
            StreamExecutionEnvironment environment,
            Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>> resolver) {
        return translateWithLookupSources(
                inputs, inputTypes, outputTypes, bytes, stateIds, environment, resolver, Map.of());
    }

    public static List<Transformation<RowData>> translateWithLookupSources(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            List<RowType> outputTypes,
            byte[] bytes,
            List<Long> stateIds,
            StreamExecutionEnvironment environment,
            Function<List<Transformation<?>>, Map<Long, FlinkOperatorMemoryShare>> resolver,
            Map<Long, tech.streamfusion.flink.arrow.CsvLookupSnapshotSource> sources) {
        var lookupSources = new tech.streamfusion.flink.join.NativeLookupSources(sources);
        var plan = decode(bytes);
        NativeSharedRegionOutputs.validate(plan, outputTypes.size());
        if (inputs.size() != plan.getInputCount() || inputTypes.size() != inputs.size())
            throw new IllegalArgumentException("Shared region inputs must match its physical input ports");
        if (environment.getConfig().isLatencyTrackingConfigured()
                && environment.getConfig().getLatencyTrackingInterval() > 0)
            throw new IllegalArgumentException(
                    "Shared native regions do not yet support Flink sampled latency routing");
        var resources = resolver == null ? NativeLocalWindowResources.NONE : NativeLocalWindowResources.pending(plan);
        Function<List<byte[]>, StreamFusionNativeRegionOperatorFactory> factoryBuilder = exchanges -> {
            var factory = StreamFusionNativeRegionOperatorFactory.shared(
                    inputTypes, outputTypes, bytes, stateIds, exchanges, resources);
            factory.withLookupSources(lookupSources);
            return resolver == null ? factory : factory.withResourceResolver(resolver);
        };
        Transformation<RowData> owner;
        if (!stateIds.isEmpty()) {
            owner = NativeKeyedRegionTranslation.translate(inputs, inputTypes, stateIds, environment, factoryBuilder);
        } else if (!NativeRegionInput.isExchange(inputs.get(0))) {
            var input = StreamFusionArrowBoundaries.toArrow(inputs.get(0), inputTypes.get(0));
            var factory = factoryBuilder.apply(List.of(NativeExchangePlanSerializer.singleton(inputTypes.get(0))));
            var result = new OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch>(
                    input,
                    "streamfusion-native-region[shared]",
                    factory,
                    ArrowRowDataBatchTypeInfo.INSTANCE,
                    input.getParallelism(),
                    false);
            result.declareManagedMemoryUseCaseAtOperatorScope(
                    ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
            owner = StreamFusionArrowBoundaries.asPlannerTransformation(result);
        } else {
            var binding = NativeRegionInput.bind(inputs.get(0), inputTypes.get(0), false);
            var result = new MultipleInputTransformation<>(
                    "streamfusion-native-region[shared]",
                    factoryBuilder.apply(List.of(binding.exchangePlan)),
                    ArrowRowDataBatchTypeInfo.INSTANCE,
                    inputs.get(0).getParallelism(),
                    false);
            result.declareManagedMemoryUseCaseAtOperatorScope(
                    ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
            result.addInput(binding.frames);
            owner = StreamFusionArrowBoundaries.asPlannerTransformation(result);
        }
        var outputs = new ArrayList<Transformation<RowData>>();
        outputs.add(owner);
        for (int port = 1; port < outputTypes.size(); port++) {
            var output = new SideOutputTransformation<>(owner, NativeSharedRegionOutputs.tag(port));
            if (owner.getMaxParallelism() > 0) output.setMaxParallelism(owner.getMaxParallelism());
            outputs.add(StreamFusionArrowBoundaries.asPlannerTransformation(output));
        }
        return List.copyOf(outputs);
    }

    private static NativeRegionPlan decode(byte[] bytes) {
        try {
            return NativeRegionPlan.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid shared region contract", failure);
        }
    }
}
