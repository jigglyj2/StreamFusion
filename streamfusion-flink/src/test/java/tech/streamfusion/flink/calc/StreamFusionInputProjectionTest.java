/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.StructField;

class StreamFusionInputProjectionTest {
    @Test
    void retainsAndRemapsOnlyFirstStageInputReferences() {
        RowType inputType = RowType.of(new BigIntType(), new VarCharType(), new IntType(), new VarCharType());
        Expression fourth = StreamFusionCalcPlan.inputReference(3, StreamFusionCalcPlan.logicalType(inputType, 3));
        Expression second = StreamFusionCalcPlan.inputReference(1, StreamFusionCalcPlan.logicalType(inputType, 1));

        StreamFusionInputProjection.Projection projection =
                StreamFusionInputProjection.create(inputType, List.of(fourth), second);

        assertThat(projection.fieldPaths()).isDeepEqualTo(new int[][] {{1}, {3}});
        assertThat(projection.rowArities()).isDeepEqualTo(new int[][] {{}, {}});
        assertThat(projection.inputType().getFieldCount()).isEqualTo(2);
        assertThat(projection.projections().get(0).getInputReference().getIndex())
                .isEqualTo(1);
        assertThat(projection.condition().getInputReference().getIndex()).isZero();
    }

    @Test
    void flattensNestedLeafReferencesAndPropagatesNullableParents() {
        RowType person = RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {
                    new BigIntType(false), new VarCharType(false, VarCharType.MAX_LENGTH), new VarCharType()
                },
                new String[] {"id", "name", "city"});
        RowType inputType = RowType.of(new IntType(), person.copy(true));
        Expression root = StreamFusionCalcPlan.inputReference(1, StreamFusionCalcPlan.logicalType(inputType, 1));
        Expression city = field(root, "city");
        Expression name = field(root, "name");

        StreamFusionInputProjection.Projection projection =
                StreamFusionInputProjection.create(inputType, List.of(name), city);

        assertThat(projection.fieldPaths()).isDeepEqualTo(new int[][] {{1, 1}, {1, 2}});
        assertThat(projection.rowArities()).isDeepEqualTo(new int[][] {{3}, {3}});
        assertThat(projection.inputType().getTypeAt(0).isNullable()).isTrue();
        assertThat(projection.projections().get(0).hasInputReference()).isTrue();
        assertThat(projection.projections().get(0).getInputReference().getIndex())
                .isZero();
        assertThat(projection.condition().getInputReference().getIndex()).isEqualTo(1);
    }

    private static Expression field(Expression operand, String name) {
        return Expression.newBuilder()
                .setStructField(StructField.newBuilder().setOperand(operand).setFieldName(name))
                .build();
    }
}
