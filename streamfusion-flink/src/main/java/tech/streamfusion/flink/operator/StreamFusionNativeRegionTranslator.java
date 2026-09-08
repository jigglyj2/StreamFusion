/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.calc.StreamFusionInputProjection;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Composes Java-planned physical trees before creating a single native runtime transformation. */
public final class StreamFusionNativeRegionTranslator {
    private StreamFusionNativeRegionTranslator() {}

    public static byte[] inputPlan(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Native region input index must be non-negative");
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setInput(tech.streamfusion.proto.plan.v1.Input.newBuilder()
                                .setInputIndex(index)))
                .build()
                .toByteArray();
    }

    /** Creates one runtime owner for an already-composed tree, translating only external edges. */
    public static Transformation<RowData> translateInputs(
            List<Transformation<RowData>> inputs, List<RowType> inputTypes, RowType outputType, byte[] plan) {
        return translateInputsWithResources(inputs, inputTypes, outputType, plan, null);
    }

    /** Original local capacities are completed only when Flink has the complete pipeline. */
    public static Transformation<RowData> translateInputsWithResources(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            java.util.function.Function<
                            List<Transformation<?>>,
                            java.util.Map<Long, tech.streamfusion.flink.memory.FlinkOperatorMemoryShare>>
                    resolver) {
        if (inputs.isEmpty() || inputs.size() != inputTypes.size()) {
            throw new IllegalArgumentException("Native region external inputs and types must have matching arity");
        }
        if (inputs.size() == 1 && !NativeRegionInput.isExchange(inputs.get(0))) {
            // No network exchange exists here: the source adapter hands its Arrow batch
            // directly to the shared runtime's sole port through Arrow C Data.
            var arrowInput = StreamFusionArrowBoundaries.toArrow(inputs.get(0), inputTypes.get(0));
            var factory = new StreamFusionNativeRegionOperatorFactory(
                    inputTypes,
                    outputType,
                    plan,
                    List.of(),
                    List.of(tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.singleton(inputTypes.get(0))),
                    resolver == null
                            ? tech.streamfusion.flink.window.NativeLocalWindowResources.NONE
                            : tech.streamfusion.flink.window.NativeLocalWindowResources.pending(plan));
            if (resolver != null) factory.withResourceResolver(resolver);
            var result = new OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch>(
                    arrowInput,
                    "streamfusion-native-region[inputs=1]",
                    factory,
                    ArrowRowDataBatchTypeInfo.INSTANCE,
                    inputs.get(0).getParallelism(),
                    false);
            result.declareManagedMemoryUseCaseAtOperatorScope(
                    ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
            return StreamFusionArrowBoundaries.asPlannerTransformation(result);
        }
        int parallelism =
                inputs.stream().mapToInt(Transformation::getParallelism).max().orElseThrow();
        java.util.ArrayList<NativeRegionInput> bindings = new java.util.ArrayList<>();
        for (int index = 0; index < inputs.size(); index++) {
            bindings.add(NativeRegionInput.bind(inputs.get(index), inputTypes.get(index), false));
        }
        var factory = new StreamFusionNativeRegionOperatorFactory(
                inputTypes,
                outputType,
                plan,
                List.of(),
                bindings.stream().map(binding -> binding.exchangePlan).collect(java.util.stream.Collectors.toList()),
                resolver == null
                        ? tech.streamfusion.flink.window.NativeLocalWindowResources.NONE
                        : tech.streamfusion.flink.window.NativeLocalWindowResources.pending(plan));
        if (resolver != null) factory.withResourceResolver(resolver);
        var result = new org.apache.flink.streaming.api.transformations.MultipleInputTransformation<>(
                "streamfusion-native-region[inputs=" + inputs.size() + "]",
                factory,
                ArrowRowDataBatchTypeInfo.INSTANCE,
                parallelism,
                false);
        result.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        for (NativeRegionInput binding : bindings) result.addInput(binding.frames);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    /** Persistent stages use the same tree and runtime; only Flink's keyed lifecycle binding differs. */
    public static Transformation<RowData> translateKeyedInputs(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment) {
        return NativeKeyedRegionTranslation.translate(inputs, inputTypes, outputType, plan, stateIds, environment);
    }

    public static Transformation<RowData> translateKeyedInputsWithResources(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
            java.util.function.Function<
                            List<Transformation<?>>,
                            java.util.Map<Long, tech.streamfusion.flink.memory.FlinkOperatorMemoryShare>>
                    resolver) {
        return NativeKeyedRegionTranslation.translate(
                inputs, inputTypes, outputType, plan, stateIds, environment, resolver);
    }

    public static Transformation<RowData> translate(
            Transformation<RowData> input, RowType inputType, RowType outputType, List<byte[]> stages) {
        if (stages.isEmpty()) {
            throw new IllegalArgumentException("A native region must contain at least one stage");
        }
        Transformation<ArrowRowDataBatch> arrowInput;
        NativePlan first = decode(stages.get(0));
        if (!StreamFusionArrowBoundaries.isArrow(input)
                && first.getProtocolVersion() >= 2
                && first.getRoot().hasCalc()
                && first.getRoot().getCalc().getPreserveInputEnvelope()) {
            var calc = first.getRoot().getCalc();
            var projection = StreamFusionInputProjection.create(
                    inputType, calc.getProjectionsList(), calc.hasCondition() ? calc.getCondition() : null);
            var rewritten = calc.toBuilder().clearProjections().addAllProjections(projection.projections());
            if (projection.condition() != null) rewritten.setCondition(projection.condition());
            // Keep physical identities, control policy and all subsequent stages intact. Only
            // the first Calc's input layout changes; computation still runs in DataFusion.
            stages = new java.util.ArrayList<>(stages);
            stages.set(
                    0,
                    first.toBuilder()
                            .setRoot(first.getRoot().toBuilder().setCalc(rewritten))
                            .build()
                            .toByteArray());
            inputType = projection.inputType();
            arrowInput = StreamFusionArrowBoundaries.toArrow(
                    input, inputType, projection.fieldPaths(), projection.rowArities());
        } else {
            arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        }
        byte[] plan = compose(stages);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> result = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-native-region[" + stages.size() + "]",
                StreamFusionArrowNativeOperator.forRegion(inputType, outputType, plan, "streamfusion-native-region"),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        result.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    public static byte[] compose(List<byte[]> stages) {
        if (stages.isEmpty()) {
            throw new IllegalArgumentException("A native region must contain at least one stage");
        }
        return composeAbove(null, stages);
    }

    /** Uses a disjoint identity range for Java physical nodes; low IDs remain available for
     * native synthetic input/control nodes. Region growth must not renumber physical stages. */
    public static byte[] identifyStage(byte[] fragment, int physicalNodeId) {
        return identifyStage(fragment, physicalNodeId, "");
    }

    public static byte[] identifyStage(byte[] fragment, int physicalNodeId, String metricName) {
        return identifyStage(fragment, physicalNodeId, metricName, null);
    }

    public static byte[] identifyStage(byte[] fragment, int physicalNodeId, String metricName, String metricUid) {
        if (physicalNodeId < 0) {
            throw new IllegalArgumentException("A Flink physical node identity must be non-negative");
        }
        NativePlan plan = decode(fragment);
        Operator stage = plan.getRoot();
        NativePlanComposer.inputCount(stage);
        long identity = (1L << 32) | Integer.toUnsignedLong(physicalNodeId);
        var identified = stage.toBuilder()
                .setPlanNodeId(identity)
                .setMetricName(metricName)
                .clearMetricUid();
        if (metricUid != null) identified.setMetricUid(metricUid);
        return plan.toBuilder().setRoot(identified).build().toByteArray();
    }

    /** Attaches unary fragments to an existing native subtree, without inspecting its family. */
    public static byte[] composeAbove(byte[] upstream, List<byte[]> stages) {
        NativePlan base = upstream == null ? null : decode(upstream);
        Operator root = base == null ? null : base.getRoot();
        int version = base == null ? 1 : base.getProtocolVersion();
        // An existing subtree can produce explicit RowKinds even when no Calc follows it.
        // Its downstream stages require the v2 envelope-aware native kernels as a unit.
        if (base != null && !stages.isEmpty()) {
            version = 2;
        }
        for (byte[] bytes : stages) {
            NativePlan plan = decode(bytes);
            version = Math.max(version, plan.getProtocolVersion());
            Operator stage = plan.getRoot();
            root = connectUnary(stage, root);
        }
        if (root == null) {
            throw new IllegalArgumentException("A native region must contain at least one stage");
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(version)
                .setRoot(root)
                .build()
                .toByteArray();
    }

    /** Binds explicit local Input slots to native subtrees. Subtree input indices are already
     * global edge indices and are preserved; no input is translated through a Java operator. */
    public static byte[] composeWithInputs(byte[] fragment, List<byte[]> inputs) {
        NativePlan plan = decode(fragment);
        int version = inputs.isEmpty() ? plan.getProtocolVersion() : Math.max(2, plan.getProtocolVersion());
        java.util.ArrayList<Operator> children = new java.util.ArrayList<>();
        for (byte[] input : inputs) {
            NativePlan child = decode(input);
            version = Math.max(version, child.getProtocolVersion());
            children.add(child.getRoot());
        }
        return plan.toBuilder()
                .setProtocolVersion(version)
                .setRoot(NativePlanComposer.bind(plan.getRoot(), children))
                .build()
                .toByteArray();
    }

    private static NativePlan decode(byte[] bytes) {
        final NativePlan plan;
        try {
            plan = NativePlan.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native region stage", failure);
        }
        if ((plan.getProtocolVersion() < 1 || plan.getProtocolVersion() > 3) || !plan.hasRoot()) {
            throw new IllegalArgumentException("Unsupported native region stage protocol");
        }
        validateRecordPolicy(plan.getRoot(), plan.getProtocolVersion());
        return plan;
    }

    private static void validateRecordPolicy(Operator stage, int protocol) {
        if (stage.getClearRecordTimestamps() && protocol < 3) {
            throw new IllegalArgumentException("Record timestamp policy requires native plan protocol 3");
        }
        for (Operator child : tech.streamfusion.flink.proto.NativePhysicalPlan.children(stage)) {
            validateRecordPolicy(child, protocol);
        }
    }

    /** Protocol-shape adaptation, not a list of supported operator pairs. Semantic admission and
     * the runtime's state bindings remain the responsibility of the selected physical nodes. */
    private static Operator connectUnary(Operator stage, Operator upstream) {
        final int count;
        try {
            count = NativePlanComposer.inputCount(stage);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("A unary region stage must declare exactly one physical child", failure);
        }
        if (count != 1) {
            throw new IllegalArgumentException("A unary region stage must declare exactly one physical child");
        }
        return upstream == null ? stage : NativePlanComposer.bind(stage, List.of(upstream));
    }
}
