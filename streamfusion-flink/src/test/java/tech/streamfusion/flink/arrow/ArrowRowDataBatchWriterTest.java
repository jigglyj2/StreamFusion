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
import org.apache.arrow.vector.IntVector;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
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
    void writesOnlyProjectedTopLevelFieldsWithoutARowWrapper() {
        RowType projectedType = RowType.of(new VarCharType(), new IntType());
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatchWriter writer = new ArrowRowDataBatchWriter(projectedType, allocator)) {
            writer.write(
                    GenericRowData.of(11, StringData.fromString("discarded"), StringData.fromString("kept"), 42),
                    new int[] {2, 3});

            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                assertThat(batch.root().getFieldVectors()).hasSize(2);
                assertThat(batch.rowView(0).getString(0).toString()).isEqualTo("kept");
                assertThat(batch.rowView(0).getInt(1)).isEqualTo(42);
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

    @Test
    void overwritesNullabilityWithoutClearingReusableDataBuffers() {
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatchWriter writer = new ArrowRowDataBatchWriter(ROW_TYPE, allocator, 16)) {
            writer.write(GenericRowData.of(1, null));
            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                IntVector integers = (IntVector) batch.root().getVector(0);
                integers.getDataBuffer().setInt(10L * Integer.BYTES, 0x12345678);
                assertThat(batch.rowView(0).isNullAt(1)).isTrue();
            }

            writer.reset();
            writer.write(GenericRowData.of(null, StringData.fromString("present")));
            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                assertThat(batch.rowView(0).isNullAt(0)).isTrue();
                assertThat(batch.rowView(0).getString(1).toString()).isEqualTo("present");
                assertThat(((IntVector) batch.root().getVector(0))
                                .getDataBuffer()
                                .getInt(10L * Integer.BYTES))
                        .isEqualTo(0x12345678);
            }
        }
    }

    @Test
    void writesSegmentedBinaryStringsDirectlyAndGrowsPastInitialCapacity() {
        MemorySegment[] segments = {
            MemorySegmentFactory.wrap("__ab".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
            MemorySegmentFactory.wrap("cdef".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        };
        BinaryStringData segmented = BinaryStringData.fromAddress(segments, 2, 6);
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatchWriter writer = new ArrowRowDataBatchWriter(ROW_TYPE, allocator, 1)) {
            writer.write(GenericRowData.of(1, segmented));
            writer.write(GenericRowData.of(2, StringData.fromString("a string longer than the initial density")));
            writer.write(GenericRowData.of(3, StringData.fromString("last")));

            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                assertThat(batch.rowView(0).getString(1).toString()).isEqualTo("abcdef");
                assertThat(batch.rowView(1).getString(1).toString())
                        .isEqualTo("a string longer than the initial density");
                assertThat(batch.rowView(2).getString(1).toString()).isEqualTo("last");
            }
        }
    }

    @Test
    void resetsValidityAcrossRetainedNestedCapacity() {
        RowType nestedType = RowType.of(new ArrayType(new IntType()));
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatchWriter writer = new ArrowRowDataBatchWriter(nestedType, allocator, 1)) {
            writer.write(GenericRowData.of(new GenericArrayData(new Integer[] {null, null, null, null})));
            try (ArrowRowDataBatch ignored = writer.finishBatch()) {
                // Grow the child vector and leave null bits beyond the configured row batch size.
            }

            writer.reset();
            writer.write(GenericRowData.of(new GenericArrayData(new Integer[] {1, 2, 3, 4})));
            try (ArrowRowDataBatch batch = writer.finishBatch()) {
                for (int index = 0; index < 4; index++) {
                    assertThat(batch.rowView(0).getArray(0).isNullAt(index)).isFalse();
                    assertThat(batch.rowView(0).getArray(0).getInt(index)).isEqualTo(index + 1);
                }
            }
        }
    }
}
