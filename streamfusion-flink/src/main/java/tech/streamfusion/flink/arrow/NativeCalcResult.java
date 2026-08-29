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

/** A native output batch and the input-row ordinal selected for each output row. */
public final class NativeCalcResult implements AutoCloseable {
    private final ArrowRowDataBatch batch;
    private final int[] inputRows;

    NativeCalcResult(ArrowRowDataBatch batch, int[] inputRows) {
        this.batch = batch;
        this.inputRows = inputRows;
    }

    public ArrowRowDataBatch batch() {
        return batch;
    }

    public int inputRow(int outputRow) {
        return inputRows[outputRow];
    }

    /** Propagates Flink record metadata while leaving the data entirely Arrow-backed. */
    public ArrowRowDataBatch selectEnvelopeFrom(ArrowRowDataBatch input) {
        return batch.selectEnvelopeFrom(input, inputRows);
    }

    @Override
    public void close() {
        batch.close();
    }
}
