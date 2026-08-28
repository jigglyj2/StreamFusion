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

class NativePlanTest {
    @Test
    void identityCalcRoundTripsThroughProtobuf() throws Exception {
        LogicalType integerType = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Operator input = Operator.newBuilder()
                .setInput(Input.newBuilder()
                        .setSchema(Schema.newBuilder()
                                .addFields(Field.newBuilder().setName("id").setType(integerType))))
                .build();
        NativePlan plan = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setCalc(Calc.newBuilder()
                                .setInput(input)
                                .addProjections(Expression.newBuilder()
                                        .setInputReference(InputReference.newBuilder()
                                                .setIndex(0)
                                                .setType(integerType)))))
                .build();

        NativePlan decoded = NativePlan.parseFrom(plan.toByteArray());

        assertThat(decoded).isEqualTo(plan);
        assertThat(decoded.getRoot()
                        .getCalc()
                        .getProjections(0)
                        .getInputReference()
                        .getIndex())
                .isZero();
    }

    @Test
    void typedNullLiteralRoundTripsThroughProtobuf() throws Exception {
        LogicalType decimalType = LogicalType.newBuilder()
                .setNullable(true)
                .setDecimal(DecimalType.newBuilder().setPrecision(20).setScale(4))
                .build();
        Expression expression = Expression.newBuilder()
                .setNullLiteral(NullLiteral.newBuilder().setType(decimalType))
                .build();

        assertThat(Expression.parseFrom(expression.toByteArray())).isEqualTo(expression);
    }

    @Test
    void fixedWidthTypesRoundTripThroughProtobuf() throws Exception {
        LogicalType binary = LogicalType.newBuilder()
                .setNullable(true)
                .setFixedBinary(LengthType.newBuilder().setLength(8))
                .build();
        LogicalType character = LogicalType.newBuilder()
                .setNullable(true)
                .setFixedChar(LengthType.newBuilder().setLength(5))
                .build();

        assertThat(LogicalType.parseFrom(binary.toByteArray())).isEqualTo(binary);
        assertThat(LogicalType.parseFrom(character.toByteArray())).isEqualTo(character);
    }
}
