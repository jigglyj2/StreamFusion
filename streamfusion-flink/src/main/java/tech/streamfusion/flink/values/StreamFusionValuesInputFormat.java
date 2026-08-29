/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.values;

import java.io.IOException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowValuesCDataBridge;

/** One-split source that exposes a source-free native VALUES Arrow batch as Flink RowData views. */
final class StreamFusionValuesInputFormat extends GenericInputFormat<RowData> implements NonParallelInput {
    private static final long serialVersionUID = 1L;

    private final byte[] serializedPlan;
    private final RowType outputType;
    private transient RootAllocator allocator;
    private transient ArrowRowDataBatch batch;
    private transient int nextRow;

    StreamFusionValuesInputFormat(byte[] serializedPlan, RowType outputType) {
        this.serializedPlan = serializedPlan;
        this.outputType = outputType;
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);
        allocator = new RootAllocator(Long.MAX_VALUE);
        try {
            batch = ArrowValuesCDataBridge.execute(serializedPlan, outputType, allocator);
        } catch (RuntimeException error) {
            allocator.close();
            allocator = null;
            throw error;
        }
    }

    @Override
    public boolean reachedEnd() {
        return nextRow >= batch.size();
    }

    @Override
    public RowData nextRecord(RowData reuse) {
        return reachedEnd() ? null : batch.rowView(nextRow++);
    }

    @Override
    public void close() throws IOException {
        try {
            if (batch != null) {
                batch.close();
                batch = null;
            }
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
        } finally {
            super.close();
        }
    }
}
