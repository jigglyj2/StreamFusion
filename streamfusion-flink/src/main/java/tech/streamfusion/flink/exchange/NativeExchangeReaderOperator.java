/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Imports Arrow IPC frames and restores their Arrow payload plus Flink envelope sidecar. */
public final class NativeExchangeReaderOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<NativeExchangeFrame, ArrowRowDataBatch> {
    private final RowType rowType;
    private final byte[] serializedPlan;
    private final NativeExchangeFrameDecoder decoder;
    private transient FlinkManagedMemory managedMemory;

    public NativeExchangeReaderOperator(RowType rowType, byte[] serializedPlan) {
        this(rowType, serializedPlan, NativeExchangeFrameDecoder.JNI);
    }

    NativeExchangeReaderOperator(RowType rowType, byte[] serializedPlan, NativeExchangeFrameDecoder decoder) {
        this.rowType = rowType;
        this.serializedPlan = serializedPlan.clone();
        this.decoder = decoder;
    }

    /** Returns the immutable exchange contract for consumers that fuse frame decoding downstream. */
    public byte[] serializedPlan() {
        return serializedPlan.clone();
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-exchange-reader");
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) {
        try (ArrowExchangeInputBatch batch =
                decoder.decode(serializedPlan, element.getValue(), rowType, managedMemory.allocator(), managedMemory)) {
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
            output.collect(new StreamRecord<>(batch.arrowBatch()));
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (managedMemory != null) {
                managedMemory.close();
                managedMemory = null;
            }
        } finally {
            super.close();
        }
    }
}
