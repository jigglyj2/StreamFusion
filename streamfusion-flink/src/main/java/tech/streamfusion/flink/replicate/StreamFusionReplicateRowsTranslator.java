/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.replicate;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.proto.plan.v1.Expression;

/** Native implementation of Flink's optimizer-only {@code $REPLICATE_ROWS$1} correlate. */
public final class StreamFusionReplicateRowsTranslator {
    public static final String FUNCTION_NAME = "$REPLICATE_ROWS$1";

    private StreamFusionReplicateRowsTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            Object joinType,
            Object invocation,
            Object condition) {
        if (unsupportedReason(inputType, outputType, joinType, invocation, condition) != null) {
            return null;
        }
        List<Expression> expressions = expressions(inputType, (RexCall) invocation);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-replicate-rows",
                new StreamFusionArrowNativeOperator(
                        outputType,
                        StreamFusionReplicateRowsPlan.create(
                                expressions.get(0), expressions.subList(1, expressions.size())),
                        "streamfusion-replicate-rows"),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType, RowType outputType, Object joinType, Object invocation, Object condition) {
        String joinName = joinType instanceof Enum<?> ? ((Enum<?>) joinType).name() : String.valueOf(joinType);
        if (!"INNER".equals(joinName)) {
            return "REPLICATE_ROWS correlate requires an inner join";
        }
        if (condition != null) {
            return "REPLICATE_ROWS correlate conditions are not accelerated";
        }
        if (!(invocation instanceof RexCall)) {
            return "REPLICATE_ROWS invocation is not a RexCall";
        }
        RexCall call = (RexCall) invocation;
        if (!FUNCTION_NAME.equals(call.getOperator().getName())) {
            return "table function " + call.getOperator().getName() + " is not StreamFusion REPLICATE_ROWS";
        }
        List<RexNode> operands = call.getOperands();
        if (operands.size() < 2) {
            return "REPLICATE_ROWS requires a BIGINT count and at least one value";
        }
        LogicalType repetitionType =
                FlinkTypeFactory.toLogicalType(operands.get(0).getType());
        if (repetitionType.getTypeRoot() != LogicalTypeRoot.BIGINT) {
            return "REPLICATE_ROWS count must be BIGINT, got " + repetitionType;
        }
        if (outputType.getFieldCount() != inputType.getFieldCount() + operands.size() - 1) {
            return "REPLICATE_ROWS output must preserve its input and append every value argument";
        }
        for (int index = 0; index < inputType.getFieldCount(); index++) {
            if (!inputType.getTypeAt(index).equals(outputType.getTypeAt(index))) {
                return "REPLICATE_ROWS output does not preserve input field " + index + " exactly";
            }
        }
        for (int index = 0; index < operands.size(); index++) {
            LogicalType operandType =
                    FlinkTypeFactory.toLogicalType(operands.get(index).getType());
            if (index > 0 && !operandType.equals(outputType.getTypeAt(inputType.getFieldCount() + index - 1))) {
                return "REPLICATE_ROWS output field " + (index - 1) + " does not match value argument " + index;
            }
            if (StreamFusionCalcTranslator.operatorExpression(operands.get(index), inputType, operandType) == null) {
                return "REPLICATE_ROWS argument " + index + " cannot be translated exactly";
            }
        }
        return null;
    }

    public static List<Expression> expressions(RowType inputType, RexCall invocation) {
        List<Expression> expressions = new ArrayList<>(invocation.getOperands().size());
        for (RexNode operand : invocation.getOperands()) {
            LogicalType type = FlinkTypeFactory.toLogicalType(operand.getType());
            expressions.add(StreamFusionCalcTranslator.operatorExpression(operand, inputType, type));
        }
        return expressions;
    }
}
