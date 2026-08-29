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

class StringHashCodePlanTest {
    @Test
    void operandRoundTrips() throws Exception {
        Expression expression = Expression.newBuilder()
                .setStringHashCode(StringHashCode.newBuilder()
                        .setOperand(Expression.newBuilder()
                                .setInputReference(InputReference.newBuilder().setIndex(0))))
                .build();

        assertThat(Expression.parseFrom(expression.toByteArray())).isEqualTo(expression);
    }
}
