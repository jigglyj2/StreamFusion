/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringTrimPlanTest {
    @Test
    void expressionValuedTrimRoundTrips() throws Exception {
        Expression expression = Expression.newBuilder()
                .setStringTrim(StringTrim.newBuilder()
                        .setValue(input(0))
                        .setCharacters(input(1))
                        .setDirection(StringTrimDirection.STRING_TRIM_DIRECTION_LEADING))
                .build();

        assertThat(Expression.parseFrom(expression.toByteArray())).isEqualTo(expression);
    }

    private static Expression input(int index) {
        return Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(index))
                .build();
    }
}
