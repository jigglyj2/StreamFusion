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
    private final org.apache.flink.types.RowKind[] nativeRowKinds;
    private final boolean ownedEnvelope;

    NativeCalcResult(ArrowRowDataBatch batch, int[] inputRows) {
        this(batch, inputRows, null);
    }

    NativeCalcResult(ArrowRowDataBatch batch, int[] inputRows, org.apache.flink.types.RowKind[] nativeRowKinds) {
        this(batch, inputRows, nativeRowKinds, false);
    }

    NativeCalcResult(
            ArrowRowDataBatch batch,
            int[] inputRows,
            org.apache.flink.types.RowKind[] nativeRowKinds,
            boolean ownedEnvelope) {
        this.batch = batch;
        this.inputRows = inputRows;
        this.nativeRowKinds = nativeRowKinds;
        this.ownedEnvelope = ownedEnvelope;
        if (nativeRowKinds != null) batch.withRowKinds(nativeRowKinds);
    }

    public ArrowRowDataBatch batch() {
        return batch;
    }

    public int inputRow(int outputRow) {
        return inputRows[outputRow];
    }

    /** Propagates Flink record metadata while leaving the data entirely Arrow-backed. */
    public ArrowRowDataBatch selectEnvelopeFrom(ArrowRowDataBatch input) {
        if (ownedEnvelope) return batch;
        batch.selectEnvelopeFrom(input, inputRows);
        return nativeRowKinds == null ? batch : batch.withRowKinds(nativeRowKinds);
    }

    /** Selects global input ordinals across a multi-input edge without touching payload columns. */
    public ArrowRowDataBatch selectEnvelopeFrom(java.util.List<ArrowRowDataBatch> inputs) {
        if (ownedEnvelope) return batch;
        if (inputs.size() == 1) {
            return selectEnvelopeFrom(inputs.get(0));
        }
        int[] ends = new int[inputs.size()];
        int total = 0;
        for (int index = 0; index < inputs.size(); index++) {
            total = Math.addExact(total, inputs.get(index).size());
            ends[index] = total;
        }
        org.apache.flink.types.RowKind[] kinds = new org.apache.flink.types.RowKind[inputRows.length];
        boolean[] timestampPresent = new boolean[inputRows.length];
        long[] timestamps = new long[inputRows.length];
        for (int row = 0; row < inputRows.length; row++) {
            int ordinal = inputRows[row];
            if (ordinal < 0 || ordinal >= total) {
                throw new IllegalStateException("Native output has an invalid global input ordinal: " + ordinal);
            }
            int low = 0;
            int high = ends.length;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (ends[middle] <= ordinal) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            ArrowRowDataBatch input = inputs.get(low);
            int localRow = ordinal - (low == 0 ? 0 : ends[low - 1]);
            kinds[row] = input.rowKind(localRow);
            timestampPresent[row] = input.hasTimestamp(localRow);
            timestamps[row] = input.timestamp(localRow);
        }
        return batch.withEnvelope(nativeRowKinds == null ? kinds : nativeRowKinds, timestampPresent, timestamps);
    }

    @Override
    public void close() {
        batch.close();
    }
}
