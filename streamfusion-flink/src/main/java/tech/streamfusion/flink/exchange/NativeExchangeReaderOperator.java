/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/** Imports Arrow IPC frames and emits lightweight RowData views with their original envelope. */
public final class NativeExchangeReaderOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<NativeExchangeFrame, RowData> {
    private final RowType rowType;
    private final byte[] serializedPlan;
    private final NativeExchangeFrameDecoder decoder;

    public NativeExchangeReaderOperator(RowType rowType, byte[] serializedPlan) {
        this(rowType, serializedPlan, NativeExchangeFrameDecoder.JNI);
    }

    NativeExchangeReaderOperator(RowType rowType, byte[] serializedPlan, NativeExchangeFrameDecoder decoder) {
        this.rowType = rowType;
        this.serializedPlan = serializedPlan.clone();
        this.decoder = decoder;
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowExchangeInputBatch batch =
                        decoder.decode(serializedPlan, element.getValue(), rowType, allocator)) {
            for (int row = 0; row < batch.size(); row++) {
                RowData value = batch.rowView(row);
                output.collect(
                        batch.hasTimestamp(row)
                                ? new StreamRecord<>(value, batch.timestamp(row))
                                : new StreamRecord<>(value));
            }
        }
    }
}
