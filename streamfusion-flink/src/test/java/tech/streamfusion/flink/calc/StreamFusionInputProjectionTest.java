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

class StreamFusionInputProjectionTest {
    @Test
    void retainsAndRemapsOnlyFirstStageInputReferences() {
        RowType inputType = RowType.of(new BigIntType(), new VarCharType(), new IntType(), new VarCharType());
        Expression fourth = StreamFusionCalcPlan.inputReference(3, StreamFusionCalcPlan.logicalType(inputType, 3));
        Expression second = StreamFusionCalcPlan.inputReference(1, StreamFusionCalcPlan.logicalType(inputType, 1));

        StreamFusionInputProjection.Projection projection =
                StreamFusionInputProjection.create(inputType, List.of(fourth), second);

        assertThat(projection.fieldOrdinals()).containsExactly(1, 3);
        assertThat(projection.inputType().getFieldCount()).isEqualTo(2);
        assertThat(projection.projections().get(0).getInputReference().getIndex())
                .isEqualTo(1);
        assertThat(projection.condition().getInputReference().getIndex()).isZero();
    }
}
