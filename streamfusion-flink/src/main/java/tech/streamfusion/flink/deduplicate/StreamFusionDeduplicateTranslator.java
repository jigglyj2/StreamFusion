/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for the timer-free rowtime keep-last deduplicate slice. */
public final class StreamFusionDeduplicateTranslator {
    private StreamFusionDeduplicateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean outputInsertOnly,
            boolean generateUpdateBefore,
            long stateRetentionTime,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        String reason = unsupportedReason(
                inputType,
                outputType,
                uniqueKeys,
                isRowtime,
                keepLastRow,
                outputInsertOnly,
                generateUpdateBefore,
                stateRetentionTime,
                config);
        if (reason != null) {
            return null;
        }
        StreamFusionStateBackendFactory.install(environment);
        int rowtimeIndex = rowtimeIndex(inputType);
        boolean generateInsert =
                config.get(ExecutionConfigOptions.TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-deduplicate[keep-last]",
                new StreamFusionArrowDeduplicateOperator(
                        inputType, uniqueKeys, rowtimeIndex, generateInsert, keySelector),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        transformation.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
        transformation.setStateKeyType(keySelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType,
            RowType outputType,
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean outputInsertOnly,
            boolean generateUpdateBefore,
            long stateRetentionTime,
            ReadableConfig config) {
        if (!isRowtime) {
            return "ordering: the initial native deduplicate requires ROWTIME DESC";
        }
        if (!keepLastRow) {
            return "ordering: keep-first deduplicate requires event-time timers";
        }
        if (outputInsertOnly) {
            return "changelog: keep-last deduplicate must produce updating output";
        }
        if (generateUpdateBefore) {
            return "changelog: Arrow-native deduplicate cannot materialize UPDATE_BEFORE rows yet";
        }
        if (stateRetentionTime != 0) {
            return "state: native deduplicate TTL is not implemented";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: native deduplicate mini-batching is not implemented";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native deduplicate";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native deduplicate";
        }
        if (!inputType.equals(outputType)) {
            return "schema: deduplicate input and output rows must match exactly";
        }
        if (uniqueKeys.length == 0) {
            return "key: deduplicate requires at least one partition key";
        }
        for (int key : uniqueKeys) {
            if (key < 0 || key >= inputType.getFieldCount()) {
                return "key: index " + key + " is outside the input row";
            }
            if (!supportedKey(inputType.getTypeAt(key))) {
                return "key[" + key + "]: " + inputType.getTypeAt(key) + " has no Flink BinaryRow parity";
            }
        }
        int rowtimeIndex = rowtimeIndex(inputType);
        if (rowtimeIndex < 0) {
            return "ordering: Flink marked deduplicate as rowtime but its input has no ROWTIME field";
        }
        for (int index = 0; index < inputType.getFieldCount(); index++) {
            if (!supportedValue(inputType.getTypeAt(index))) {
                return "state row[" + index + "]: " + inputType.getTypeAt(index)
                        + " is not supported by the initial Arrow row codec";
            }
        }
        return null;
    }

    private static int rowtimeIndex(RowType rowType) {
        for (int index = 0; index < rowType.getFieldCount(); index++) {
            if (TypeCheckUtils.isRowTime(rowType.getTypeAt(index))) {
                return index;
            }
        }
        return -1;
    }

    private static boolean supportedKey(LogicalType type) {
        return supportedValue(type);
    }

    private static boolean supportedValue(LogicalType type) {
        if (type instanceof DistinctType) {
            return supportedValue(((DistinctType) type).getSourceType());
        }
        if (type instanceof StructuredType) {
            return type.getChildren().stream().allMatch(StreamFusionDeduplicateTranslator::supportedValue);
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
                return true;
            default:
                return false;
        }
    }
}
