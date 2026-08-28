/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.unnest.StreamFusionArrayUnnestTranslator;
import tech.streamfusion.proto.plan.v1.Expression;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
public final class StreamFusionCalcTranslator extends StreamFusionExpressionTranslator {
    private StreamFusionCalcTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            List<?> projections,
            Object condition) {
        if (unsupportedReason(inputType, outputType, projections, condition) != null) {
            return null;
        }

        return translateChain(
                input,
                java.util.Collections.singletonList(inputType),
                java.util.Collections.singletonList(outputType),
                java.util.Collections.singletonList(projections),
                java.util.Collections.singletonList(condition));
    }

    /** Translates adjacent Flink Calc nodes into one native operator and one nested native plan. */
    public static Transformation<RowData> translateChain(
            Transformation<RowData> input,
            List<RowType> inputTypes,
            List<RowType> outputTypes,
            List<List<?>> projectionStages,
            List<?> conditions) {
        if (inputTypes.isEmpty()
                || inputTypes.size() != outputTypes.size()
                || inputTypes.size() != projectionStages.size()
                || inputTypes.size() != conditions.size()) {
            throw new IllegalArgumentException("A native Calc chain must contain equally sized, non-empty stages");
        }
        List<List<Expression>> nativeProjectionStages = new ArrayList<>(projectionStages.size());
        List<Expression> nativeConditions = new ArrayList<>(conditions.size());
        for (int stage = 0; stage < inputTypes.size(); stage++) {
            RowType stageInputType = inputTypes.get(stage);
            RowType stageOutputType = outputTypes.get(stage);
            List<?> stageProjections = projectionStages.get(stage);
            if (unsupportedReason(stageInputType, stageOutputType, stageProjections, conditions.get(stage)) != null) {
                return null;
            }
            List<Expression> nativeProjections = new ArrayList<>(stageProjections.size());
            for (int outputIndex = 0; outputIndex < stageProjections.size(); outputIndex++) {
                nativeProjections.add(projectionExpression(
                        stageProjections.get(outputIndex), stageInputType, stageOutputType.getTypeAt(outputIndex)));
            }
            nativeProjectionStages.add(nativeProjections);
            nativeConditions.add(conditionExpression(conditions.get(stage), stageInputType));
        }
        RowType outputType = outputTypes.get(outputTypes.size() - 1);
        StreamFusionIdentityCalcOperator operator = new StreamFusionIdentityCalcOperator(
                inputTypes.get(0), outputType, nativeProjectionStages, nativeConditions);
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-calc-chain[" + inputTypes.size() + "]",
                operator,
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    /** Fuses an array UNNEST and every immediately following Calc into one native plan. */
    public static Transformation<RowData> translateArrayUnnestChain(
            Transformation<RowData> input,
            RowType boundaryInputType,
            RowType unnestOutputType,
            Object invocation,
            List<RowType> inputTypes,
            List<RowType> outputTypes,
            List<List<?>> projectionStages,
            List<?> conditions) {
        if (StreamFusionArrayUnnestTranslator.unsupportedReason(
                        boundaryInputType, unnestOutputType, "INNER", invocation, null)
                != null) {
            return null;
        }
        if (inputTypes.isEmpty() || !inputTypes.get(0).equals(unnestOutputType)) {
            throw new IllegalArgumentException("The first Calc input must equal the fused UNNEST output");
        }
        List<List<Expression>> nativeProjectionStages = new ArrayList<>(projectionStages.size());
        List<Expression> nativeConditions = new ArrayList<>(conditions.size());
        for (int stage = 0; stage < inputTypes.size(); stage++) {
            RowType stageInputType = inputTypes.get(stage);
            RowType stageOutputType = outputTypes.get(stage);
            List<?> stageProjections = projectionStages.get(stage);
            if (unsupportedReason(stageInputType, stageOutputType, stageProjections, conditions.get(stage)) != null) {
                return null;
            }
            List<Expression> nativeProjections = new ArrayList<>(stageProjections.size());
            for (int outputIndex = 0; outputIndex < stageProjections.size(); outputIndex++) {
                nativeProjections.add(projectionExpression(
                        stageProjections.get(outputIndex), stageInputType, stageOutputType.getTypeAt(outputIndex)));
            }
            nativeProjectionStages.add(nativeProjections);
            nativeConditions.add(conditionExpression(conditions.get(stage), stageInputType));
        }
        RowType outputType = outputTypes.get(outputTypes.size() - 1);
        StreamFusionIdentityCalcOperator operator = new StreamFusionIdentityCalcOperator(
                boundaryInputType,
                outputType,
                StreamFusionArrayUnnestTranslator.arrayIndex(invocation),
                unnestOutputType.getFieldCount(),
                nativeProjectionStages,
                nativeConditions);
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-array-unnest-calc-chain[" + inputTypes.size() + "]",
                operator,
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    public static boolean canTranslate(RowType inputType, RowType outputType, List<?> projections, Object condition) {
        return unsupportedReason(inputType, outputType, projections, condition) == null;
    }

    /** Returns {@code null} when supported, otherwise the first precise expression-path rejection. */
    public static String unsupportedReason(
            RowType inputType, RowType outputType, List<?> projections, Object condition) {
        if (projections.isEmpty()) {
            return "projection: a Calc must produce at least one column";
        }
        if (outputType.getFieldCount() != projections.size()) {
            return "projection: Flink produced "
                    + projections.size()
                    + " expressions for "
                    + outputType.getFieldCount()
                    + " output fields";
        }
        for (int outputIndex = 0; outputIndex < projections.size(); outputIndex++) {
            Object projection = projections.get(outputIndex);
            org.apache.flink.table.types.logical.LogicalType expectedType = outputType.getTypeAt(outputIndex);
            int directInput = inputIndex(projection);
            if (directInput >= 0
                    && (directInput >= inputType.getFieldCount()
                            || !isSupportedProjectionType(
                                    inputType.getTypeAt(directInput).getTypeRoot())
                            || !inputType.getTypeAt(directInput).equals(expectedType))) {
                return "projection["
                        + outputIndex
                        + "]/input["
                        + directInput
                        + "]: input and output types must match exactly";
            }
            if (projectionExpression(projection, inputType, expectedType) == null) {
                return expressionFailure(projection, inputType, expectedType, false, "projection[" + outputIndex + "]");
            }
        }
        if (condition != null && conditionExpression(condition, inputType) == null) {
            return expressionFailure(
                    condition,
                    inputType,
                    new org.apache.flink.table.types.logical.BooleanType(true),
                    true,
                    "condition");
        }
        return null;
    }
}
