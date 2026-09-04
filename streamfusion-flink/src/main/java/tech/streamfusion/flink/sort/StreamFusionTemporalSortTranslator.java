/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.ArrowUtils;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native event-time and processing-time temporal sort. */
public final class StreamFusionTemporalSortTranslator {
    private StreamFusionTemporalSortTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            SortSpec sortSpec,
            boolean processingTime,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        String reason = unsupportedReason(inputType, sortSpec, processingTime, config);
        if (reason != null) {
            return null;
        }
        if (processingTime && sortSpec.getFieldSize() == 1) {
            return input;
        }
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> singleton = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            singleton = StreamFusionExchangeTranslator.singleton(input, inputType);
        }
        byte[] plan = StreamFusionTemporalSortPlan.create(inputType, sortSpec, processingTime);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(singleton, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-temporal-sort[" + (processingTime ? "proctime" : "rowtime") + "]",
                new StreamFusionArrowTemporalSortOperator(inputType, processingTime, plan),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                1,
                false);
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 2);
        transformation.setStateKeySelector(new ArrowBatchKeySelector(EmptyRowDataKeySelector.INSTANCE));
        transformation.setStateKeyType(EmptyRowDataKeySelector.INSTANCE.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType, SortSpec sortSpec, boolean processingTime, ReadableConfig config) {
        if (sortSpec.getFieldSize() == 0) {
            return "sort: temporal sort requires a time ordering field";
        }
        SortSpec.SortFieldSpec time = sortSpec.getFieldSpec(0);
        if (!time.getIsAscendingOrder()) {
            return "sort: Flink temporal sort requires ascending time";
        }
        if (time.getFieldIndex() < 0 || time.getFieldIndex() >= inputType.getFieldCount()) {
            return "sort: temporal sort time index is outside the input row";
        }
        LogicalTypeRoot timeRoot = inputType.getTypeAt(time.getFieldIndex()).getTypeRoot();
        if (!processingTime
                && timeRoot != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                && timeRoot != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                && timeRoot != LogicalTypeRoot.BIGINT) {
            return "sort: row-time temporal sort requires a timestamp or BIGINT time attribute";
        }
        for (int index = 1; index < sortSpec.getFieldSize(); index++) {
            int fieldIndex = sortSpec.getFieldSpec(index).getFieldIndex();
            if (fieldIndex < 0 || fieldIndex >= inputType.getFieldCount()) {
                return "sort: secondary field index " + fieldIndex + " is outside the input row";
            }
            if (!orderable(inputType.getTypeAt(fieldIndex))) {
                return "sort: secondary field " + fieldIndex + " type " + inputType.getTypeAt(fieldIndex)
                        + " has no exact native Flink comparator";
            }
        }
        try {
            ArrowUtils.toArrowSchema(inputType);
            for (LogicalType type : inputType.getChildren()) {
                FlinkLogicalTypeProto.serialize(type);
            }
        } catch (RuntimeException failure) {
            return "schema: " + failure.getMessage();
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native temporal sort";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native temporal sort";
        }
        return null;
    }

    private static boolean orderable(LogicalType type) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return orderable(((ArrayType) type).getElementType());
            case ROW:
                for (LogicalType child : type.getChildren()) {
                    if (!orderable(child)) {
                        return false;
                    }
                }
                return true;
            case MAP:
            case MULTISET:
            case RAW:
            case SYMBOL:
            case DESCRIPTOR:
                return false;
            default:
                return true;
        }
    }
}
