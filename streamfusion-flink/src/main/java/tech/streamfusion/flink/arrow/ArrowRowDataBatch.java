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
import org.apache.flink.types.RowKind;

/**
 * Owns one Arrow batch plus Flink's per-record envelope metadata.
 *
 * <p>This is the runtime payload between StreamFusion operators. RowData views exist only for the
 * source/sink boundary and Flink control logic; native operators consume {@link #root()} directly.
 */
public final class ArrowRowDataBatch implements AutoCloseable {
    private static final long TEST_ALLOCATOR_LIMIT = 64L * 1024 * 1024;
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    private final boolean ownsRoot;
    private final VectorSchemaRoot root;
    private final RowType rowType;
    private final ArrowReader reader;
    /** Null arrays represent the overwhelmingly common INSERT/no-record-timestamp envelope. */
    private RowKind[] rowKinds;

    private boolean[] hasTimestamps;
    private long[] timestamps;

    private ArrowRowDataBatch(
            BufferAllocator allocator,
            boolean ownsAllocator,
            boolean ownsRoot,
            VectorSchemaRoot root,
            RowType rowType) {
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
        this.ownsRoot = ownsRoot;
        this.root = root;
        this.rowType = rowType;
        this.reader = ArrowUtils.createArrowReader(root, rowType);
    }

    static ArrowRowDataBatch transpose(List<? extends RowData> rows, RowType rowType) {
        BufferAllocator allocator = new RootAllocator(TEST_ALLOCATOR_LIMIT);
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
            return new ArrowRowDataBatch(allocator, ownsAllocator, true, root, rowType);
        } catch (RuntimeException | Error failure) {
            root.close();
            if (ownsAllocator) {
                allocator.close();
            }
            throw failure;
        }
    }

    static ArrowRowDataBatch wrap(VectorSchemaRoot root, RowType rowType) {
        return new ArrowRowDataBatch(vectorAllocator(root), false, true, root, rowType);
    }

    static ArrowRowDataBatch wrap(VectorSchemaRoot root, RowType rowType, BufferAllocator allocator) {
        return new ArrowRowDataBatch(allocator, false, true, root, rowType);
    }

    public static ArrowRowDataBatch borrowed(VectorSchemaRoot root, RowType rowType) {
        return new ArrowRowDataBatch(vectorAllocator(root), false, false, root, rowType);
    }

    public static ArrowRowDataBatch borrowed(VectorSchemaRoot root, RowType rowType, BufferAllocator allocator) {
        return new ArrowRowDataBatch(allocator, false, false, root, rowType);
    }

    public RowType rowType() {
        return rowType;
    }

    /** Attaches source-envelope metadata without copying any Arrow buffers. */
    public ArrowRowDataBatch withEnvelope(RowKind[] kinds, boolean[] timestampPresence, long[] recordTimestamps) {
        int rows = size();
        if (kinds.length < rows || timestampPresence.length < rows || recordTimestamps.length < rows) {
            throw new IllegalArgumentException("Arrow batch envelope is shorter than the batch");
        }
        boolean nonInsert = false;
        boolean anyTimestamp = false;
        for (int row = 0; row < rows && (!nonInsert || !anyTimestamp); row++) {
            nonInsert |= kinds[row] != RowKind.INSERT;
            anyTimestamp |= timestampPresence[row];
        }
        this.rowKinds = nonInsert ? java.util.Arrays.copyOf(kinds, rows) : null;
        this.hasTimestamps = anyTimestamp ? java.util.Arrays.copyOf(timestampPresence, rows) : null;
        this.timestamps = anyTimestamp ? java.util.Arrays.copyOf(recordTimestamps, rows) : null;
        return this;
    }

    /** Selects/duplicates the input envelope according to a native output's input ordinals. */
    public ArrowRowDataBatch selectEnvelopeFrom(ArrowRowDataBatch input, int[] inputRows) {
        if (inputRows.length != size()) {
            throw new IllegalArgumentException("Native selection length does not match the Arrow output");
        }
        if (input.hasTrivialEnvelope()) {
            return this;
        }
        RowKind[] selectedKinds = input.rowKinds == null ? null : new RowKind[inputRows.length];
        boolean[] selectedTimestampPresence = input.hasTimestamps == null ? null : new boolean[inputRows.length];
        long[] selectedTimestamps = input.timestamps == null ? null : new long[inputRows.length];
        for (int outputRow = 0; outputRow < inputRows.length; outputRow++) {
            int inputRow = inputRows[outputRow];
            if (inputRow < 0 || inputRow >= input.size()) {
                throw new IllegalStateException("Native operator returned invalid input-row ordinal " + inputRow);
            }
            if (selectedKinds != null) {
                selectedKinds[outputRow] = input.rowKinds[inputRow];
            }
            if (selectedTimestampPresence != null) {
                selectedTimestampPresence[outputRow] = input.hasTimestamps[inputRow];
                selectedTimestamps[outputRow] = input.timestamps[inputRow];
            }
        }
        this.rowKinds = selectedKinds;
        this.hasTimestamps = selectedTimestampPresence;
        this.timestamps = selectedTimestamps;
        return this;
    }

    /** Replaces changelog kinds while retaining selected record timestamps. */
    public ArrowRowDataBatch withRowKinds(RowKind[] kinds) {
        int rows = size();
        if (kinds.length != rows) {
            throw new IllegalArgumentException("Arrow RowKind count does not match the batch");
        }
        boolean nonInsert = false;
        for (int row = 0; row < rows && !nonInsert; row++) {
            nonInsert = kinds[row] != RowKind.INSERT;
        }
        this.rowKinds = nonInsert ? java.util.Arrays.copyOf(kinds, rows) : null;
        return this;
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

    /** Allocator that owns this batch's Arrow buffers and must be used when exporting C Data. */
    public BufferAllocator allocator() {
        if (allocator == null) {
            throw new IllegalStateException("An empty Arrow batch must retain its owning allocator");
        }
        return allocator;
    }

    public int size() {
        return root.getRowCount();
    }

    public RowKind rowKind(int row) {
        return rowKinds == null ? RowKind.INSERT : rowKinds[row];
    }

    public boolean hasTimestamp(int row) {
        return hasTimestamps != null && hasTimestamps[row];
    }

    public long timestamp(int row) {
        return timestamps == null ? Long.MIN_VALUE : timestamps[row];
    }

    public boolean hasTrivialEnvelope() {
        return rowKinds == null && hasTimestamps == null;
    }

    /** Clears StreamRecord timestamps for Flink operators whose outputs never carry them. */
    public ArrowRowDataBatch withoutTimestamps() {
        this.hasTimestamps = null;
        this.timestamps = null;
        return this;
    }

    /** Creates a Java-safe Arrow slice, copying only buffers Arrow Java cannot represent sliced. */
    public ArrowRowDataBatch slice(int offset, int length) {
        return slice(offset, length, allocator());
    }

    /**
     * Creates a Java-safe Arrow slice using a compatible allocator.
     *
     * <p>Arrow can retain or transfer a buffer only within the allocator tree that already owns it.
     * Runtime operators should therefore use {@link #slice(int, int)}; the explicit allocator form
     * is retained for boundary tests and callers that already share the same root allocator.
     */
    public ArrowRowDataBatch slice(int offset, int length, BufferAllocator allocator) {
        VectorSchemaRoot slicedRoot = ArrowBatchRebaser.rebase(root, offset, length, allocator);
        ArrowRowDataBatch sliced = wrap(slicedRoot, rowType);
        sliced.rowKinds = rowKinds == null ? null : java.util.Arrays.copyOfRange(rowKinds, offset, offset + length);
        sliced.hasTimestamps =
                hasTimestamps == null ? null : java.util.Arrays.copyOfRange(hasTimestamps, offset, offset + length);
        sliced.timestamps =
                timestamps == null ? null : java.util.Arrays.copyOfRange(timestamps, offset, offset + length);
        return sliced;
    }

    @Override
    public void close() {
        if (ownsRoot) {
            root.close();
        }
        if (ownsAllocator) {
            allocator.close();
        }
    }

    private static BufferAllocator vectorAllocator(VectorSchemaRoot root) {
        return root.getFieldVectors().isEmpty()
                ? null
                : root.getFieldVectors().get(0).getAllocator();
    }
}
