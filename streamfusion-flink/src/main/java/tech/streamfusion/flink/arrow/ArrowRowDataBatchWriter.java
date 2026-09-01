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
    private static final int DEFAULT_BATCH_CAPACITY = 1024;
    private static final int MINIMUM_ADAPTIVE_CAPACITY = 64;

    private final RowType rowType;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final ArrowWriter<RowData> writer;
    private final int batchCapacity;
    private int rowCount;
    private boolean finished;

    public ArrowRowDataBatchWriter(RowType rowType, BufferAllocator allocator) {
        this(rowType, allocator, DEFAULT_BATCH_CAPACITY);
    }

    public ArrowRowDataBatchWriter(RowType rowType, BufferAllocator allocator, int batchCapacity) {
        this.rowType = rowType;
        this.allocator = allocator;
        this.batchCapacity = batchCapacity;
        VectorSchemaRoot createdRoot = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
        try {
            this.writer = ArrowUtils.createRowDataArrowWriter(createdRoot, rowType, batchCapacity);
            this.root = createdRoot;
        } catch (RuntimeException | Error failure) {
            try {
                createdRoot.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** Finds the largest power-of-two batch capacity that fits the assigned Arrow allowance. */
    static ArrowRowDataBatchWriter createAdaptive(
            RowType rowType, BufferAllocator allocator, int maximumBatchCapacity) {
        int capacity = maximumBatchCapacity;
        while (true) {
            try {
                return new ArrowRowDataBatchWriter(rowType, allocator, capacity);
            } catch (org.apache.arrow.memory.OutOfMemoryException unavailable) {
                if (capacity <= MINIMUM_ADAPTIVE_CAPACITY) {
                    throw unavailable;
                }
                capacity = Math.max(MINIMUM_ADAPTIVE_CAPACITY, capacity / 2);
            }
        }
    }

    /** Copies one row's field values directly into their Arrow column buffers. */
    public void write(RowData row) {
        if (finished) {
            throw new IllegalStateException("Reset the Arrow batch writer before writing another batch");
        }
        writer.write(row);
        rowCount++;
    }

    /** Copies only selected top-level fields into their compact Arrow schema. */
    public void write(RowData row, int[] fieldOrdinals) {
        if (finished) {
            throw new IllegalStateException("Reset the Arrow batch writer before writing another batch");
        }
        writer.write(row, fieldOrdinals);
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

    int batchCapacity() {
        return batchCapacity;
    }

    @Override
    public void close() {
        root.close();
    }
}
