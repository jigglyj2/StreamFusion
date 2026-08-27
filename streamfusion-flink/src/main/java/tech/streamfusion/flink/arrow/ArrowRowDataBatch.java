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

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Owns a transposed Arrow batch and exposes reusable, non-materializing Flink row views. */
public final class ArrowRowDataBatch implements AutoCloseable {
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private final VectorSchemaRoot root;
    private final ArrowReader reader;

    private ArrowRowDataBatch(
            BufferAllocator allocator, boolean ownsAllocator, VectorSchemaRoot root, RowType rowType) {
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
        this.root = root;
        this.reader = ArrowUtils.createArrowReader(root, rowType);
    }

    public static ArrowRowDataBatch transpose(List<? extends RowData> rows, RowType rowType) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        return transpose(rows, rowType, allocator, true);
    }

    /** Transposes rows using a caller-owned allocator, allowing Flink to account for off-heap use. */
    public static ArrowRowDataBatch transpose(
            List<? extends RowData> rows, RowType rowType, BufferAllocator allocator) {
        return transpose(rows, rowType, allocator, false);
    }

    private static ArrowRowDataBatch transpose(
            List<? extends RowData> rows, RowType rowType, BufferAllocator allocator, boolean ownsAllocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
        try {
            ArrowWriter<RowData> writer = ArrowUtils.createRowDataArrowWriter(root, rowType);
            rows.forEach(writer::write);
            writer.finish();
            return new ArrowRowDataBatch(allocator, ownsAllocator, root, rowType);
        } catch (RuntimeException | Error failure) {
            root.close();
            if (ownsAllocator) {
                allocator.close();
            }
            throw failure;
        }
    }

    /** Returns the reusable row view used by PyFlink's Arrow reader model. */
    public RowData rowView(int rowId) {
        if (rowId < 0 || rowId >= size()) {
            throw new IndexOutOfBoundsException("Arrow row " + rowId + " outside batch of " + size());
        }
        return reader.read(rowId);
    }

    public VectorSchemaRoot root() {
        return root;
    }

    public int size() {
        return root.getRowCount();
    }

    @Override
    public void close() {
        root.close();
        if (ownsAllocator) {
            allocator.close();
        }
    }
}
