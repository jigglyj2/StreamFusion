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
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Operator;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
public final class StreamFusionCalcTranslator extends StreamFusionExpressionTranslator {
    private StreamFusionCalcTranslator() {}

    /** Reuses Calc's parity-checked expression contract for another native physical operator. */
    public static Expression operatorExpression(
            Object expression, RowType inputType, org.apache.flink.table.types.logical.LogicalType expectedType) {
        return projectionExpression(expression, inputType, expectedType);
    }

    /** Reuses Calc's nullable-boolean/SQL-UNKNOWN contract for an operator predicate. */
    public static Expression operatorCondition(Object expression, RowType inputType) {
        return conditionExpression(expression, inputType);
    }

    /** Returns Calc's precise fallback reason for a predicate embedded in another operator. */
    public static String operatorConditionFailure(Object expression, RowType inputType, String path) {
        return expressionFailure(expression, inputType, null, true, path);
    }

    /** Reuses Calc's parity-checked Flink-to-protobuf type mapping. */
    public static tech.streamfusion.proto.plan.v1.LogicalType operatorLogicalType(
            org.apache.flink.table.types.logical.LogicalType type) {
        return StreamFusionCalcPlan.logicalType(type);
    }

    /** Builds a Calc tail for a native changelog operator without introducing another JNI edge. */
    public static Operator appendChangelogCalcOperators(
            Operator input,
            List<RowType> inputTypes,
            List<RowType> outputTypes,
            List<List<?>> projectionStages,
            List<?> conditions) {
        if (inputTypes.isEmpty()) {
            throw new IllegalArgumentException("A fused changelog Calc tail must not be empty");
        }
        NativeCalcStages stages = nativeCalcStages(inputTypes, outputTypes, projectionStages, conditions);
        if (stages == null) {
            return null;
        }
        return StreamFusionCalcPlan.appendChangelogCalcOperators(
                input, inputTypes.get(0).getFieldCount(), stages.projections, stages.conditions);
    }

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

    /** Plans one physical stage; a region builder supplies its native child before execution. */
    public static byte[] createStagePlan(RowType inputType, RowType outputType, List<?> projections, Object condition) {
        NativeCalcStages stages = nativeCalcStages(
                List.of(inputType),
                List.of(outputType),
                List.of(projections),
                java.util.Collections.singletonList(condition));
        if (stages == null) {
            throw new IllegalArgumentException("A selected native Calc stage failed semantic validation");
        }
        return StreamFusionCalcPlan.createStage(stages.projections.get(0), stages.conditions.get(0));
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
        RowType planInputType = inputTypes.get(0);
        Transformation<ArrowRowDataBatch> arrowInput;
        if (StreamFusionArrowBoundaries.isArrow(input)) {
            arrowInput = StreamFusionArrowBoundaries.toArrow(input, planInputType);
        } else {
            StreamFusionInputProjection.Projection inputProjection = StreamFusionInputProjection.create(
                    planInputType, nativeProjectionStages.get(0), nativeConditions.get(0));
            planInputType = inputProjection.inputType();
            nativeProjectionStages.set(0, inputProjection.projections());
            nativeConditions.set(0, inputProjection.condition());
            arrowInput = StreamFusionArrowBoundaries.toArrow(
                    input, planInputType, inputProjection.fieldPaths(), inputProjection.rowArities());
        }
        byte[] plan = StreamFusionCalcPlan.create(planInputType, nativeProjectionStages, nativeConditions);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-calc-chain[" + inputTypes.size() + "]",
                new StreamFusionArrowNativeOperator(outputType, plan, "streamfusion-calc"),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    private static NativeCalcStages nativeCalcStages(
            List<RowType> inputTypes, List<RowType> outputTypes, List<List<?>> projectionStages, List<?> conditions) {
        if (inputTypes.size() != outputTypes.size()
                || inputTypes.size() != projectionStages.size()
                || inputTypes.size() != conditions.size()) {
            throw new IllegalArgumentException("A native Calc chain must contain equally sized stages");
        }
        List<List<Expression>> nativeProjections = new ArrayList<>(projectionStages.size());
        List<Expression> nativeConditions = new ArrayList<>(conditions.size());
        for (int stage = 0; stage < inputTypes.size(); stage++) {
            RowType inputType = inputTypes.get(stage);
            RowType outputType = outputTypes.get(stage);
            List<?> projections = projectionStages.get(stage);
            if (unsupportedReason(inputType, outputType, projections, conditions.get(stage)) != null) {
                return null;
            }
            List<Expression> expressions = new ArrayList<>(projections.size());
            for (int outputIndex = 0; outputIndex < projections.size(); outputIndex++) {
                expressions.add(projectionExpression(
                        projections.get(outputIndex), inputType, outputType.getTypeAt(outputIndex)));
            }
            nativeProjections.add(expressions);
            nativeConditions.add(conditionExpression(conditions.get(stage), inputType));
        }
        return new NativeCalcStages(nativeProjections, nativeConditions);
    }

    private static final class NativeCalcStages {
        private final List<List<Expression>> projections;
        private final List<Expression> conditions;

        private NativeCalcStages(List<List<Expression>> projections, List<Expression> conditions) {
            this.projections = projections;
            this.conditions = conditions;
        }
    }

    public static boolean canTranslate(RowType inputType, RowType outputType, List<?> projections, Object condition) {
        return unsupportedReason(inputType, outputType, projections, condition) == null;
    }

    /** Returns {@code null} when supported, otherwise the first precise expression-path rejection. */
    public static String unsupportedReason(
            RowType inputType, RowType outputType, List<?> projections, Object condition) {
        for (int index = 0; index < inputType.getFieldCount(); index++) {
            String reason = StreamFusionTimestampRangeSupport.unsupportedReason(
                    inputType.getTypeAt(index), "input[" + index + "]");
            if (reason != null) {
                return reason;
            }
        }
        for (int index = 0; index < outputType.getFieldCount(); index++) {
            String reason = StreamFusionTimestampRangeSupport.unsupportedReason(
                    outputType.getTypeAt(index), "output[" + index + "]");
            if (reason != null) {
                return reason;
            }
        }
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
                            || !sameTypeIgnoringNullability(inputType.getTypeAt(directInput), expectedType))) {
                return "projection["
                        + outputIndex
                        + "]/input["
                        + directInput
                        + "]: input and output types must match except for nullability (input="
                        + (directInput < inputType.getFieldCount() ? inputType.getTypeAt(directInput) : "out of range")
                        + ", output=" + expectedType + ")";
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

    private static boolean sameTypeIgnoringNullability(
            org.apache.flink.table.types.logical.LogicalType input,
            org.apache.flink.table.types.logical.LogicalType output) {
        // FlinkTypeFactory normalizes interval qualifiers/precision through Calcite.
        // An identity RexInputRef still forwards the original months/milliseconds;
        // this is not a CAST and must not truncate or reinterpret its value.
        if (input.getTypeRoot() == output.getTypeRoot()
                && (input.getTypeRoot() == org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME
                        || input.getTypeRoot()
                                == org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH)) {
            return true;
        }
        return input.copy(true).equals(output.copy(true));
    }
}
