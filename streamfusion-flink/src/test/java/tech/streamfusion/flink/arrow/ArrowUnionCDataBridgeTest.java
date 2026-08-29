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
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

class ArrowUnionCDataBridgeTest {
    @Test
    void unionsMultipleArrowInputsAndReturnsGlobalInputOrdinals() {
        RowType rowType = RowType.of(new IntType(false));
        List<RowData> leftRows = List.of(GenericRowData.of(1), GenericRowData.of(2));
        List<RowData> rightRows = List.of(GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch left = ArrowRowDataBatch.transpose(leftRows, rowType, allocator);
                ArrowRowDataBatch right = ArrowRowDataBatch.transpose(rightRows, rowType, allocator);
                NativeCalcResult result = ArrowUnionCDataBridge.executeWithSelection(
                        unionPlan(2), List.of(left, right), rowType, allocator)) {
            assertThat(result.batch().size()).isEqualTo(3);
            assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(1);
            assertThat(result.batch().rowView(1).getInt(0)).isEqualTo(2);
            assertThat(result.batch().rowView(2).getInt(0)).isEqualTo(3);
            assertThat(result.inputRow(0)).isZero();
            assertThat(result.inputRow(1)).isEqualTo(1);
            assertThat(result.inputRow(2)).isEqualTo(2);
        }
    }

    @Test
    void acceptsAnEmptyInputWithoutChangingTheOtherInput() {
        RowType rowType = RowType.of(new IntType(false));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch left = ArrowRowDataBatch.transpose(List.of(), rowType, allocator);
                ArrowRowDataBatch right =
                        ArrowRowDataBatch.transpose(List.of(GenericRowData.of(7)), rowType, allocator);
                NativeCalcResult result = ArrowUnionCDataBridge.executeWithSelection(
                        unionPlan(2), List.of(left, right), rowType, allocator)) {
            assertThat(result.batch().size()).isOne();
            assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(7);
            assertThat(result.inputRow(0)).isZero();
        }
    }

    private static byte[] unionPlan(int inputCount) {
        Union.Builder union = Union.newBuilder();
        for (int index = 0; index < inputCount; index++) {
            union.addInputs(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(index)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setUnion(union))
                .build()
                .toByteArray();
    }
}
