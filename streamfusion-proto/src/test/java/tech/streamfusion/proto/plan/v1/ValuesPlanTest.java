/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ValuesPlanTest {
    @Test
    void roundTripsADeclaredSchemaAndNullableRows() throws Exception {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(true)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Values values = Values.newBuilder()
                .setSchema(Schema.newBuilder()
                        .addFields(Field.newBuilder().setName("id").setType(integer)))
                .addRows(ValuesRow.newBuilder()
                        .addValues(Expression.newBuilder()
                                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(7))))
                .addRows(ValuesRow.newBuilder()
                        .addValues(Expression.newBuilder()
                                .setNullLiteral(NullLiteral.newBuilder().setType(integer))))
                .build();
        NativePlan plan = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setValues(values))
                .build();

        NativePlan decoded = NativePlan.parseFrom(plan.toByteArray());

        assertThat(decoded.getRoot().getValues().getSchema().getFields(0).getName())
                .isEqualTo("id");
        assertThat(decoded.getRoot().getValues().getRows(1).getValues(0).hasNullLiteral())
                .isTrue();
    }
}
