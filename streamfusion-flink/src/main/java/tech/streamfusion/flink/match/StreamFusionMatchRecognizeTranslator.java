/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.match;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.Expression;

/** Reflection entry point for native fixed-sequence MATCH_RECOGNIZE. */
public final class StreamFusionMatchRecognizeTranslator {
    private StreamFusionMatchRecognizeTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            List<String> variableNames,
            List<?> conditions,
            int[] measureVariables,
            int[] measureFields,
            boolean skipPastLastRow,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        String reason = unsupportedReason(
                inputType,
                outputType,
                partitionKeys,
                variableNames,
                conditions,
                measureVariables,
                measureFields,
                config);
        if (reason != null) {
            return null;
        }
        List<Expression> nativeConditions = new ArrayList<>(conditions.size());
        for (Object condition : conditions) {
            nativeConditions.add(
                    condition == null
                            ? null
                            : StreamFusionCalcTranslator.operatorExpression(
                                    condition, inputType, new BooleanType(true)));
        }
        StreamFusionStateBackendFactory.install(environment);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-match-recognize",
                new StreamFusionArrowMatchRecognizeOperator(
                        inputType,
                        outputType,
                        partitionKeys,
                        variableNames,
                        nativeConditions,
                        measureVariables,
                        measureFields,
                        skipPastLastRow,
                        keySelector),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                partitionKeys.length == 0 ? 1 : input.getParallelism(),
                false);
        if (partitionKeys.length == 0) {
            transformation.setMaxParallelism(1);
        } else if (input.getMaxParallelism() > 0) {
            transformation.setMaxParallelism(input.getMaxParallelism());
        }
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        transformation.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
        transformation.setStateKeyType(keySelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            List<String> variableNames,
            List<?> conditions,
            int[] measureVariables,
            int[] measureFields,
            ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native match recognize";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native match recognize";
        }
        if (variableNames.isEmpty() || variableNames.size() != conditions.size()) {
            return "pattern: a fixed match sequence requires one condition per variable";
        }
        if (measureVariables.length != measureFields.length
                || outputType.getFieldCount() != partitionKeys.length + measureVariables.length) {
            return "measures: output must contain partition columns followed by one field per measure";
        }
        for (int index = 0; index < inputType.getFieldCount(); index++) {
            if (!supportedType(inputType.getTypeAt(index))) {
                return "input[" + index + "]: " + inputType.getTypeAt(index)
                        + " has no native Arrow-row representation";
            }
        }
        for (int index = 0; index < outputType.getFieldCount(); index++) {
            if (!supportedType(outputType.getTypeAt(index))) {
                return "output[" + index + "]: " + outputType.getTypeAt(index) + " has no native Arrow representation";
            }
        }
        for (int index = 0; index < partitionKeys.length; index++) {
            int key = partitionKeys[index];
            if (key < 0 || key >= inputType.getFieldCount()) {
                return "partition key: index " + key + " is outside the input row";
            }
            if (!sameType(inputType.getTypeAt(key), outputType.getTypeAt(index))) {
                return "partition key: output field " + index + " does not preserve input field " + key;
            }
        }
        for (int index = 0; index < conditions.size(); index++) {
            Object condition = conditions.get(index);
            if (condition != null
                    && StreamFusionCalcTranslator.operatorExpression(condition, inputType, new BooleanType(true))
                            == null) {
                return "define[" + variableNames.get(index) + "]: expression is not supported by native Calc";
            }
        }
        for (int index = 0; index < measureVariables.length; index++) {
            int variable = measureVariables[index];
            int field = measureFields[index];
            if (variable < 0 || variable >= variableNames.size() || field < 0 || field >= inputType.getFieldCount()) {
                return "measure[" + index + "]: variable or field is outside the fixed match schema";
            }
            if (!sameType(inputType.getTypeAt(field), outputType.getTypeAt(partitionKeys.length + index))) {
                return "measure[" + index + "]: output type does not match input field " + field;
            }
        }
        return null;
    }

    private static boolean sameType(LogicalType left, LogicalType right) {
        return left.copy(true).equals(right.copy(true));
    }

    private static boolean supportedType(LogicalType type) {
        if (type instanceof DistinctType) {
            return supportedType(((DistinctType) type).getSourceType());
        }
        if (type instanceof StructuredType) {
            return type.getChildren().stream().allMatch(StreamFusionMatchRecognizeTranslator::supportedType);
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
                return type.getChildren().stream().allMatch(StreamFusionMatchRecognizeTranslator::supportedType);
            default:
                return false;
        }
    }
}
