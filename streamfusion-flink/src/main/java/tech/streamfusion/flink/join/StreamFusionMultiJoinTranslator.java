/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native N-input streaming joins. */
public final class StreamFusionMultiJoinTranslator {
    private StreamFusionMultiJoinTranslator() {}

    public static Transformation<RowData> translate(
            List<Transformation<RowData>> inputs,
            List<RowType> inputTypes,
            RowType outputType,
            List<int[]> commonKeyIndices,
            List<FlinkJoinType> joinTypes,
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            List<List<int[]>> inputUniqueKeys,
            long[] stateRetentionMillis,
            boolean equiOnly,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            List<RowDataKeySelector> commonKeySelectors,
            List<Map<Integer, RowDataKeySelector>> conditionSelectors) {
        String reason = unsupportedReason(
                inputTypes,
                outputType,
                commonKeyIndices,
                joinTypes,
                joinAttributeMap,
                inputUniqueKeys,
                stateRetentionMillis,
                equiOnly,
                config);
        if (reason != null || inputs.size() != inputTypes.size()) {
            return null;
        }
        byte[] plan = StreamFusionMultiJoinPlan.create(
                inputTypes, commonKeyIndices, joinTypes, joinAttributeMap, stateRetentionMillis);
        StreamFusionStateBackendFactory.install(environment);
        List<Transformation<NativeExchangeFrame>> framedInputs = new ArrayList<>(inputs.size());
        List<byte[]> exchangePlans = new ArrayList<>(inputs.size());
        int parallelism = inputs.get(0).getParallelism();
        int maxParallelism = DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
        for (int input = 0; input < inputs.size(); input++) {
            Transformation<RowData> keyed =
                    keyed(inputs.get(input), inputTypes.get(input), commonKeyIndices.get(input), config, environment);
            parallelism = keyed.getParallelism();
            if (keyed.getMaxParallelism() > 0) {
                maxParallelism = keyed.getMaxParallelism();
            }
            FramedInput framed = framed(keyed);
            framedInputs.add(framed.transformation);
            exchangePlans.add(framed.plan);
        }
        StreamFusionMultiJoinOperatorFactory factory = new StreamFusionMultiJoinOperatorFactory(
                inputTypes, outputType, commonKeyIndices, plan, commonKeySelectors, conditionSelectors, exchangePlans);
        KeyedMultipleInputTransformation<ArrowRowDataBatch> transformation = new KeyedMultipleInputTransformation<>(
                "streamfusion-multi-join[" + inputs.size() + "]",
                factory,
                ArrowRowDataBatchTypeInfo.INSTANCE,
                parallelism,
                false,
                Types.INT);
        transformation.setMaxParallelism(maxParallelism);
        // A multi-input join simultaneously owns decoded input, persistent opaque rowsets, and a
        // potentially multiplicative output batch. Give that work a proportional share of Flink's
        // existing OPERATOR pool instead of the unary-operator weight used by calc/filter stages.
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, Math.max(2, inputTypes.size() * 2));
        NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(maxParallelism);
        framedInputs.forEach(input -> transformation.addInput(input, selector));
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            List<RowType> inputTypes,
            RowType outputType,
            List<int[]> commonKeyIndices,
            List<FlinkJoinType> joinTypes,
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            List<List<int[]>> inputUniqueKeys,
            long[] stateRetentionMillis,
            boolean equiOnly,
            ReadableConfig config) {
        int inputCount = inputTypes.size();
        if (inputCount < 2
                || commonKeyIndices.size() != inputCount
                || joinTypes.size() != inputCount
                || inputUniqueKeys.size() != inputCount
                || stateRetentionMillis.length != inputCount) {
            return "multi-join contract: input metadata sizes differ";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: native multi-join bundle semantics are not implemented yet";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native multi-join";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native multi-join";
        }
        if (!equiOnly) {
            return "join condition: native multi-join currently requires conditions fully represented by equi attributes";
        }
        if (joinTypes.isEmpty()
                || joinTypes.get(0) != FlinkJoinType.INNER
                || joinTypes.stream()
                        .skip(1)
                        .anyMatch(type -> type != FlinkJoinType.INNER && type != FlinkJoinType.LEFT)) {
            return "join type: native multi-join supports Flink's INNER/LEFT recursive join shapes";
        }
        if (inputUniqueKeys.stream().anyMatch(keys -> keys != null && !keys.isEmpty())) {
            return "upsert state: native multi-join unique-key compaction is not implemented yet";
        }
        for (long ttl : stateRetentionMillis) {
            if (ttl != 0) {
                return "state TTL: native multi-join expiration semantics are not implemented yet";
            }
        }
        int outputArity = 0;
        for (int input = 0; input < inputCount; input++) {
            RowType type = inputTypes.get(input);
            outputArity += type.getFieldCount();
            if (commonKeyIndices.get(input).length == 0) {
                return "common join key: native multi-join requires a shared partition key";
            }
            for (int field : commonKeyIndices.get(input)) {
                if (field < 0 || field >= type.getFieldCount()) {
                    return "common join key: input " + input + " field " + field + " is outside its row";
                }
            }
            for (int field = 0; field < type.getFieldCount(); field++) {
                if (!supportedType(type.getTypeAt(field))) {
                    return "input " + input + " state row[" + field + "]: " + type.getTypeAt(field)
                            + " has no native Arrow-row representation";
                }
            }
        }
        if (outputType.getFieldCount() != outputArity) {
            return "schema: multi-join output arity is not the concatenation of all inputs";
        }
        for (Map.Entry<Integer, List<ConditionAttributeRef>> entry : joinAttributeMap.entrySet()) {
            int depth = entry.getKey();
            // Flink includes an attribute-map entry for the first input even though there is no
            // preceding input to join against. It is planner bookkeeping, not a join predicate.
            if (depth == 0) {
                continue;
            }
            if (depth <= 0 || depth >= inputCount) {
                return "join attributes: depth " + depth + " is outside the input list";
            }
            for (ConditionAttributeRef condition : entry.getValue()) {
                if (condition.leftInputId < 0
                        || condition.leftInputId >= depth
                        || condition.rightInputId != depth
                        || condition.leftFieldIndex < 0
                        || condition.leftFieldIndex
                                >= inputTypes.get(condition.leftInputId).getFieldCount()
                        || condition.rightFieldIndex < 0
                        || condition.rightFieldIndex >= inputTypes.get(depth).getFieldCount()) {
                    return "join attributes: invalid field reference at depth " + depth;
                }
            }
        }
        return null;
    }

    private static boolean supportedType(LogicalType type) {
        if (type instanceof DistinctType) {
            return supportedType(((DistinctType) type).getSourceType());
        }
        if (type instanceof StructuredType) {
            return type.getChildren().stream().allMatch(StreamFusionMultiJoinTranslator::supportedType);
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                return type.getChildren().stream().allMatch(StreamFusionMultiJoinTranslator::supportedType);
            default:
                return false;
        }
    }

    private static Transformation<RowData> keyed(
            Transformation<RowData> input,
            RowType type,
            int[] keys,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        if ("StreamFusionExchangeReader".equals(input.getName())) {
            return input;
        }
        return StreamFusionExchangeTranslator.hash(
                input,
                type,
                keys,
                DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                environment.getParallelism(),
                config.get(CheckpointingOptions.ENABLE_UNALIGNED) || config.get(CheckpointingOptions.FORCE_UNALIGNED));
    }

    @SuppressWarnings("unchecked")
    private static FramedInput framed(Transformation<RowData> input) {
        if (!(input instanceof OneInputTransformation) || !"StreamFusionExchangeReader".equals(input.getName())) {
            throw new IllegalStateException("Native multi-join requires a framed exchange on every input");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native multi-join cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native multi-join received an incompatible exchange reader");
        }
        Transformation<?> framed = reader.getInputs().get(0);
        if (!(framed.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native multi-join exchange input is not frame encoded");
        }
        return new FramedInput(
                (Transformation<NativeExchangeFrame>) framed,
                ((NativeExchangeReaderOperator) operator).serializedPlan());
    }

    private static final class FramedInput {
        private final Transformation<NativeExchangeFrame> transformation;
        private final byte[] plan;

        private FramedInput(Transformation<NativeExchangeFrame> transformation, byte[] plan) {
            this.transformation = transformation;
            this.plan = plan;
        }
    }
}
