/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.types.RowKind;

/** An Arrow-native deduplicate output plus its Flink envelope selection. */
public final class NativeArrowDeduplicateResult implements AutoCloseable {
    private final ArrowRowDataBatch batch;
    private final int[] inputRows;
    private final RowKind[] rowKinds;

    NativeArrowDeduplicateResult(ArrowRowDataBatch batch, int[] inputRows, RowKind[] rowKinds) {
        this.batch = batch;
        this.inputRows = inputRows;
        this.rowKinds = rowKinds;
    }

    public int size() {
        return inputRows.length;
    }

    /** Selects input timestamps and installs the native changelog kinds without copying data. */
    public ArrowRowDataBatch selectEnvelopeFrom(ArrowRowDataBatch input) {
        return batch.selectEnvelopeFrom(input, inputRows).withRowKinds(rowKinds);
    }

    @Override
    public void close() {
        batch.close();
    }
}
