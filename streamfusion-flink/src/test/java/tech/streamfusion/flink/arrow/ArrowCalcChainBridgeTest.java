/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class ArrowCalcChainBridgeTest extends ArrowCDataBridgeTestSupport {
    @Test
    void executesAdjacentCalcsInOneNativePlanAndPreservesInputOrdinals() {
        RowType inputType = RowType.of(new IntType(false));
        RowType outputType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                NativeCalcResult result =
                        ArrowCDataBridge.executeWithSelection(chainedSelectionPlan(), input, outputType, allocator)) {
            assertThat(result.batch().size()).isEqualTo(2);
            assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(12);
            assertThat(result.batch().rowView(1).getInt(0)).isEqualTo(13);
            assertThat(result.inputRow(0)).isEqualTo(1);
            assertThat(result.inputRow(1)).isEqualTo(2);
        }
    }

    @Test
    void returnsNativeFilterSelectionAsInputRowOrdinals() {
        RowType inputType = RowType.of(new IntType(false));
        RowType outputType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(3), GenericRowData.of(1), GenericRowData.of(4));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                NativeCalcResult result =
                        ArrowCDataBridge.executeWithSelection(selectionPlan(), input, outputType, allocator)) {
            assertThat(result.batch().size()).isEqualTo(2);
            assertThat(result.batch().rowView(0).getInt(0)).isEqualTo(3);
            assertThat(result.batch().rowView(1).getInt(0)).isEqualTo(4);
            assertThat(result.inputRow(0)).isEqualTo(0);
            assertThat(result.inputRow(1)).isEqualTo(2);
        }
    }

    @Test
    void transfersRebasedNestedBatchThroughDataFusionAndBackToRowData() {
        RowType inputType = RowType.of(
                new IntType(false), new VarCharType(), new DecimalType(10, 2), new ArrayType(new VarCharType()));
        RowType outputType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(row(1, "skip", 100, "x"), row(2, "two", 200, "a"), row(3, "three", 300, "b"));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch original = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                VectorSchemaRoot rebasedRoot = ArrowBatchRebaser.rebase(original.root(), 1, 2, allocator);
                ArrowRowDataBatch rebased = ArrowRowDataBatch.wrap(rebasedRoot, inputType);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(plan(0, 2), rebased, outputType, allocator)) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getInt(0)).isEqualTo(2);
            assertThat(output.rowView(1).getInt(0)).isEqualTo(3);
        }
    }
}
