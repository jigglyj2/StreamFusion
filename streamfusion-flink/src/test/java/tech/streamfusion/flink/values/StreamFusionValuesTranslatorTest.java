/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.values;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativePlan;

class StreamFusionValuesTranslatorTest {
    @Test
    void acceptsFlinksOneRowZeroColumnSeedForSourceFreeExpressions() {
        RowType emptyOutput = RowType.of(new LogicalType[0]);

        assertThat(StreamFusionValuesTranslator.unsupportedReason(emptyOutput, List.of(List.of())))
                .isNull();
    }

    @Test
    void preservesDeclaredFieldNullabilityInTheNativeSchema() throws Exception {
        RowType output = RowType.of(
                new LogicalType[] {new IntType(false), new VarCharType(true, VarCharType.MAX_LENGTH)},
                new String[] {"id", "name"});

        NativePlan plan = NativePlan.parseFrom(StreamFusionValuesTranslator.createPlan(output, List.of()));

        assertThat(plan.getRoot().getValues().getSchema().getFieldsList())
                .extracting(field -> field.getType().getNullable())
                .containsExactly(false, true);
    }

    @Test
    void rejectsComplexValuesBeforeNativeExecution() {
        RowType output = RowType.of(new ArrayType(new IntType()));

        assertThat(StreamFusionValuesTranslator.unsupportedReason(output, List.of(List.of())))
                .isEqualTo("fields[0]: VALUES complex type ARRAY<INT> has no parity-approved native literal mapping");
    }
}
