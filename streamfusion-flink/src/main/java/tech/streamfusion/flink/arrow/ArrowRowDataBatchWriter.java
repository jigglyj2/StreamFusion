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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Reusable, directly written RowData-to-Arrow boundary batch. */
public final class ArrowRowDataBatchWriter implements AutoCloseable {
    private final RowType rowType;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final ArrowWriter<RowData> writer;
    private int rowCount;
    private boolean finished;

    public ArrowRowDataBatchWriter(RowType rowType, BufferAllocator allocator) {
        this.rowType = rowType;
        this.allocator = allocator;
        this.root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
        this.writer = ArrowUtils.createRowDataArrowWriter(root, rowType);
    }

    /** Copies one row's field values directly into their Arrow column buffers. */
    public void write(RowData row) {
        if (finished) {
            throw new IllegalStateException("Reset the Arrow batch writer before writing another batch");
        }
        writer.write(row);
        rowCount++;
    }

    /** Finishes the current vectors and borrows them until the returned batch is closed. */
    public ArrowRowDataBatch finishBatch() {
        if (finished) {
            throw new IllegalStateException("The Arrow batch writer is already finished");
        }
        writer.finish();
        finished = true;
        return ArrowRowDataBatch.borrowed(root, rowType, allocator);
    }

    /** Finishes the batch and attaches the Flink record envelopes captured at the source edge. */
    public ArrowRowDataBatch finishBatch(RowKind[] rowKinds, boolean[] hasTimestamps, long[] timestamps) {
        return finishBatch().withEnvelope(rowKinds, hasTimestamps, timestamps);
    }

    /** Clears logical values while retaining vector allocations for the next batch. */
    public void reset() {
        writer.reset();
        rowCount = 0;
        finished = false;
    }

    public int size() {
        return rowCount;
    }

    @Override
    public void close() {
        root.close();
    }
}
