/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Establishes Flink's current key for a batch already hash-partitioned by the deduplicate key. */
public final class ArrowBatchKeySelector implements KeySelector<ArrowRowDataBatch, RowData> {
    private final RowDataKeySelector rowSelector;

    public ArrowBatchKeySelector(RowDataKeySelector rowSelector) {
        this.rowSelector = rowSelector;
    }

    @Override
    public RowData getKey(ArrowRowDataBatch batch) throws Exception {
        if (batch.size() == 0) {
            throw new IllegalArgumentException("A keyed Arrow batch must not be empty");
        }
        return rowSelector.getKey(batch.rowView(0));
    }
}
