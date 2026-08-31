/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.types.RowKind;

/** Input ordinals and changelog kinds selected by one native deduplicate batch. */
public final class NativeDeduplicateResult implements AutoCloseable {
    private final int[] inputRows;
    private final RowKind[] rowKinds;
    private final byte[][] storedRows;

    NativeDeduplicateResult(int[] inputRows, RowKind[] rowKinds, byte[][] storedRows) {
        this.inputRows = inputRows;
        this.rowKinds = rowKinds;
        this.storedRows = storedRows;
    }

    public int size() {
        return inputRows.length;
    }

    public int inputRow(int index) {
        return inputRows[index];
    }

    public RowKind rowKind(int index) {
        return rowKinds[index];
    }

    public byte[] storedRow(int index) {
        return storedRows == null ? null : storedRows[index];
    }

    @Override
    public void close() {}
}
