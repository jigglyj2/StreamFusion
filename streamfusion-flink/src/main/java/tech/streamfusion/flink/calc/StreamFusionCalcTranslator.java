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

import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** Reflection entry point called by the small Flink planner patch for eligible calc nodes. */
public final class StreamFusionCalcTranslator {
    private StreamFusionCalcTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            List<RexNode> projections,
            RexNode condition) {
        if (!isIntegerCalc(inputType, outputType, projections, condition)) {
            return null;
        }

        int inputIndex = ((RexInputRef) projections.get(0)).getIndex();
        Integer minimum = minimumValue(condition, inputIndex);
        StreamFusionIdentityCalcOperator operator = new StreamFusionIdentityCalcOperator(inputIndex, minimum);
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-identity-calc",
                operator,
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    private static boolean isIntegerCalc(
            RowType inputType, RowType outputType, List<RexNode> projections, RexNode condition) {
        if (outputType.getFieldCount() != 1
                || projections.size() != 1
                || !(projections.get(0) instanceof RexInputRef)) {
            return false;
        }
        RexInputRef reference = (RexInputRef) projections.get(0);
        return (condition == null || minimumValue(condition, reference.getIndex()) != null)
                && inputType.getTypeAt(reference.getIndex()).getTypeRoot() == LogicalTypeRoot.INTEGER
                && outputType.getTypeAt(0).getTypeRoot() == LogicalTypeRoot.INTEGER
                && !inputType.getTypeAt(reference.getIndex()).isNullable()
                && !outputType.getTypeAt(0).isNullable();
    }

    private static Integer minimumValue(RexNode condition, int inputIndex) {
        if (condition == null) {
            return null;
        }
        if (!(condition instanceof RexCall) || condition.getKind() != SqlKind.GREATER_THAN_OR_EQUAL) {
            return null;
        }
        List<RexNode> operands = ((RexCall) condition).getOperands();
        if (!(operands.get(0) instanceof RexInputRef)
                || ((RexInputRef) operands.get(0)).getIndex() != inputIndex
                || !(operands.get(1) instanceof RexLiteral)) {
            return null;
        }
        return ((RexLiteral) operands.get(1)).getValueAs(Integer.class);
    }
}
