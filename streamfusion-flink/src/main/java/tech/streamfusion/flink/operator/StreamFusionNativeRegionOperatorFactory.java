/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Shared runtime factory for arrival-driven, control-preserving native regions of any arity. */
public final class StreamFusionNativeRegionOperatorFactory extends AbstractStreamOperatorFactory<ArrowRowDataBatch> {
    private final List<RowType> inputTypes;
    private final RowType outputType;
    private final byte[] plan;
    private final List<Long> stateIds;
    private final List<byte[]> exchangePlans;
    private final tech.streamfusion.flink.window.NativeLocalWindowResources localWindowResources;

    public StreamFusionNativeRegionOperatorFactory(List<RowType> inputTypes, RowType outputType, byte[] plan) {
        this(inputTypes, outputType, plan, List.of());
    }

    public StreamFusionNativeRegionOperatorFactory(
            List<RowType> inputTypes, RowType outputType, byte[] plan, List<Long> stateIds) {
        this(
                inputTypes,
                outputType,
                plan,
                stateIds,
                inputTypes.stream()
                        .map(tech.streamfusion.flink.exchange.NativeExchangePlanSerializer::singleton)
                        .collect(java.util.stream.Collectors.toList()));
    }

    public StreamFusionNativeRegionOperatorFactory(
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            List<byte[]> exchangePlans) {
        this(
                inputTypes,
                outputType,
                plan,
                stateIds,
                exchangePlans,
                tech.streamfusion.flink.window.NativeLocalWindowResources.NONE);
    }

    public StreamFusionNativeRegionOperatorFactory(
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            List<byte[]> exchangePlans,
            tech.streamfusion.flink.window.NativeLocalWindowResources localWindowResources) {
        this.inputTypes = List.copyOf(inputTypes);
        if (inputTypes.isEmpty()) {
            throw new IllegalArgumentException("An arrival-driven native region needs external inputs");
        }
        this.outputType = outputType;
        this.plan = ownedEnvelopePlan(plan);
        this.localWindowResources = java.util.Objects.requireNonNull(localWindowResources);
        localWindowResources.validate(this.plan);
        this.stateIds = List.copyOf(stateIds);
        if (exchangePlans.size() != inputTypes.size()) {
            throw new IllegalArgumentException("Native region exchange contracts must match external input arity");
        }
        this.exchangePlans = exchangePlans.stream().map(byte[]::clone).collect(java.util.stream.Collectors.toList());
    }

    // Every input is an IPC frame. Its envelope must remain native through the complete
    // execution tree, including stateless UNION trees that were composed as protocol 2.
    private static byte[] ownedEnvelopePlan(byte[] bytes) {
        try {
            var plan = tech.streamfusion.proto.plan.v1.NativePlan.parseFrom(bytes);
            if (plan.getProtocolVersion() < 1 || plan.getProtocolVersion() > 3 || !plan.hasRoot()) {
                throw new IllegalArgumentException("Unsupported native region plan protocol");
            }
            return plan.toBuilder().setProtocolVersion(3).build().toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native region plan", failure);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<ArrowRowDataBatch>> T createStreamOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters) {
        return (T) new StreamFusionArrowNativeRegionOperator(
                parameters, inputTypes, outputType, plan, stateIds, exchangePlans, localWindowResources);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return StreamFusionArrowNativeRegionOperator.class;
    }
}
