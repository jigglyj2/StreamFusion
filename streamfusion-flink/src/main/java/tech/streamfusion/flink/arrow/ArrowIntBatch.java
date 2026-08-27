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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;

/** Owned one-column Arrow batch and lightweight Flink {@link RowData} view. */
public final class ArrowIntBatch implements AutoCloseable {
    private final BufferAllocator allocator;
    private final IntVector vector;
    private final ColumnarRowData rowView;

    private ArrowIntBatch(int capacity) {
        long allocationLimit = Math.max(64L, (long) capacity * Integer.BYTES * 2);
        this.allocator = new RootAllocator(allocationLimit);
        this.vector = new IntVector("value", allocator);
        this.vector.allocateNew(capacity);
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {new ArrowIntColumnVector(vector)});
        batch.setNumRows(capacity);
        this.rowView = new ColumnarRowData(batch);
    }

    public static ArrowIntBatch fromRows(Iterable<RowData> rows, int inputIndex, int size) {
        ArrowIntBatch batch = new ArrowIntBatch(size);
        int rowId = 0;
        for (RowData row : rows) {
            batch.vector.setSafe(rowId++, row.getInt(inputIndex));
        }
        if (rowId != size) {
            batch.close();
            throw new IllegalArgumentException("Expected " + size + " rows, received " + rowId);
        }
        batch.vector.setValueCount(size);
        return batch;
    }

    public static ArrowIntBatch fromRebasedArray(int[] values) {
        ArrowIntBatch batch = new ArrowIntBatch(values.length);
        for (int rowId = 0; rowId < values.length; rowId++) {
            batch.vector.setSafe(rowId, values[rowId]);
        }
        batch.vector.setValueCount(values.length);
        return batch;
    }

    /** Materializes a zero-offset array for the transitional JNI boundary. */
    public int[] toRebasedArray() {
        int[] values = new int[vector.getValueCount()];
        for (int rowId = 0; rowId < values.length; rowId++) {
            values[rowId] = vector.get(rowId);
        }
        return values;
    }

    /** Returns a reusable view. Consume it synchronously before requesting another row. */
    public RowData rowView(int rowId) {
        rowView.setRowId(rowId);
        return rowView;
    }

    public int size() {
        return vector.getValueCount();
    }

    @Override
    public void close() {
        vector.close();
        allocator.close();
    }
}
