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

class StringEltPlanTest {
    @Test
    void dynamicIndexAndValuesRoundTrip() throws Exception {
        Expression expression = Expression.newBuilder()
                .setStringElt(StringElt.newBuilder()
                        .setIndex(input(0))
                        .addValues(input(1))
                        .addValues(input(2)))
                .build();

        assertThat(Expression.parseFrom(expression.toByteArray())).isEqualTo(expression);
    }

    private static Expression input(int index) {
        return Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(index))
                .build();
    }
}
