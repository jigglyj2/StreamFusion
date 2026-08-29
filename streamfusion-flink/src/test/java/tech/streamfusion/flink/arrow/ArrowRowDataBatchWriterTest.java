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

import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class ArrowRowDataBatchWriterTest {
    private static final RowType ROW_TYPE = RowType.of(new IntType(), new VarCharType());

    @Test
    void writesRowsBeforeAnUpstreamObjectCanBeReused() {
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatchWriter writer = new ArrowRowDataBatchWriter(ROW_TYPE, allocator)) {
            GenericRowData reused = GenericRowData.of(1, StringData.fromString("first"));
            writer.write(reused);
            reused.setField(0, 2);
            reused.setField(1, StringData.fromString("second"));
            writer.write(reused);

            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                assertThat(batch.rowView(0).getInt(0)).isEqualTo(1);
                assertThat(batch.rowView(0).getString(1).toString()).isEqualTo("first");
                assertThat(batch.rowView(1).getInt(0)).isEqualTo(2);
                assertThat(batch.rowView(1).getString(1).toString()).isEqualTo("second");
            }
        }
    }

    @Test
    void reusesTheSameManagedArrowVectorsAcrossBatches() {
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatchWriter writer = new ArrowRowDataBatchWriter(ROW_TYPE, allocator)) {
            writer.write(GenericRowData.of(1, StringData.fromString("one")));
            long firstBuffer;
            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                firstBuffer = batch.root().getVector(0).getDataBuffer().memoryAddress();
                assertThat(writer.size()).isEqualTo(1);
            }

            writer.reset();
            writer.write(GenericRowData.of(2, StringData.fromString("two")));
            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                assertThat(batch.root().getVector(0).getDataBuffer().memoryAddress())
                        .isEqualTo(firstBuffer);
                assertThat(batch.rowView(0).getInt(0)).isEqualTo(2);
            }
        }
    }
}
