/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import java.util.List;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Builds Calc plan fragments and preserves native changelog metadata. */
public final class StreamFusionCalcPlan {
    private StreamFusionCalcPlan() {}

    /** Generated Flink SQL collectors drop record timestamps, but preserve SQL rowtime fields. */
    static byte[] createStage(List<Expression> projections, Expression condition) {
        Calc.Builder calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addAllProjections(projections)
                .setPreserveInputEnvelope(true);
        if (condition != null) {
            calc.setCondition(condition);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder().setCalc(calc).setClearRecordTimestamps(true))
                .build()
                .toByteArray();
    }

    static byte[] create(RowType inputType, List<List<Expression>> projectionStages, List<Expression> conditions) {
        Operator input = Operator.newBuilder().setInput(Input.newBuilder()).build();
        return appendCalcs(input, inputType.getFieldCount(), projectionStages, conditions);
    }

    private static byte[] appendCalcs(
            Operator input, int inputFieldCount, List<List<Expression>> projectionStages, List<Expression> conditions) {
        Operator operator = appendCalcOperators(input, inputFieldCount, projectionStages, conditions);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(operator)
                .build()
                .toByteArray();
    }

    /** Appends Calc stages below another native operator while retaining the hidden row ordinal. */
    public static Operator appendCalcOperators(
            Operator input, int inputFieldCount, List<List<Expression>> projectionStages, List<Expression> conditions) {
        if (projectionStages.size() != conditions.size()) {
            throw new IllegalArgumentException("Calc projections and conditions must have the same stage count");
        }
        Operator operator = input;
        int stageInputFieldCount = inputFieldCount;
        for (int stage = 0; stage < projectionStages.size(); stage++) {
            List<Expression> projections = projectionStages.get(stage);
            Calc.Builder calc = Calc.newBuilder().setInput(operator).addAllProjections(projections);
            calc.addProjections(inputReference(
                    stageInputFieldCount, logicalType(new org.apache.flink.table.types.logical.IntType(false))));
            Expression condition = conditions.get(stage);
            if (condition != null) {
                calc.setCondition(condition);
            }
            operator = Operator.newBuilder().setCalc(calc).build();
            stageInputFieldCount = projections.size();
        }
        return operator;
    }

    /**
     * Appends Calc stages to a changelog-producing operator.
     *
     * <p>Stateful native operators carry RowKind and the input ordinal as the two columns after
     * their visible payload. Both are plan-internal metadata: SQL expressions cannot address
     * them, but every Calc stage must preserve them so the single Arrow boundary at the end of a
     * fused plan can reconstruct Flink's changelog exactly.
     */
    public static Operator appendChangelogCalcOperators(
            Operator input, int inputFieldCount, List<List<Expression>> projectionStages, List<Expression> conditions) {
        if (projectionStages.size() != conditions.size()) {
            throw new IllegalArgumentException("Calc projections and conditions must have the same stage count");
        }
        Operator operator = input;
        int stageInputFieldCount = inputFieldCount;
        for (int stage = 0; stage < projectionStages.size(); stage++) {
            List<Expression> projections = projectionStages.get(stage);
            Calc.Builder calc = Calc.newBuilder().setInput(operator).addAllProjections(projections);
            calc.addProjections(inputReference(
                    stageInputFieldCount, logicalType(new org.apache.flink.table.types.logical.TinyIntType(false))));
            calc.addProjections(inputReference(
                    stageInputFieldCount + 1, logicalType(new org.apache.flink.table.types.logical.IntType(false))));
            Expression condition = conditions.get(stage);
            if (condition != null) {
                calc.setCondition(condition);
            }
            operator = Operator.newBuilder().setCalc(calc).build();
            stageInputFieldCount = projections.size();
        }
        return operator;
    }

    static LogicalType logicalType(RowType inputType, int inputIndex) {
        return tech.streamfusion.flink.proto.FlinkLogicalTypeProto.serialize(inputType.getTypeAt(inputIndex));
    }

    static LogicalType logicalType(org.apache.flink.table.types.logical.LogicalType flinkType) {
        return tech.streamfusion.flink.proto.FlinkLogicalTypeProto.serialize(flinkType);
    }

    static Expression inputReference(int inputIndex, LogicalType type) {
        return Expression.newBuilder()
                .setInputReference(
                        InputReference.newBuilder().setIndex(inputIndex).setType(type))
                .build();
    }
}
