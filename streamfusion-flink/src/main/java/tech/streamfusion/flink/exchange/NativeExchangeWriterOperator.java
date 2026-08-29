/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Batches RowData, routes it natively by key group, and emits Arrow IPC network frames. */
public final class NativeExchangeWriterOperator extends AbstractStreamOperator<NativeExchangeFrame>
        implements OneInputStreamOperator<RowData, NativeExchangeFrame>, BoundedOneInput {
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private final RowType inputType;
    private final RowDataSerializer serializer;
    private final byte[] serializedPlan;
    private final int batchSize;
    private final NativeExchangeBatchRouter router;
    private final List<StreamRecord<RowData>> records;
    private transient FlinkManagedMemory managedMemory;

    public NativeExchangeWriterOperator(RowType inputType, byte[] serializedPlan) {
        this(inputType, serializedPlan, DEFAULT_BATCH_SIZE, NativeExchangeBatchRouter.JNI);
    }

    NativeExchangeWriterOperator(
            RowType inputType, byte[] serializedPlan, int batchSize, NativeExchangeBatchRouter router) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Native exchange batch size must be positive");
        }
        this.inputType = inputType;
        this.serializer = new RowDataSerializer(inputType);
        this.serializedPlan = serializedPlan.clone();
        this.batchSize = batchSize;
        this.router = router;
        this.records = new ArrayList<>(batchSize);
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(), getOperatorConfig(), getMetricGroup(), "streamfusion-exchange");
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = serializer.copy(element.getValue());
        records.add(element.hasTimestamp() ? new StreamRecord<>(row, element.getTimestamp()) : new StreamRecord<>(row));
        if (records.size() == batchSize) {
            flushBatch();
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        flushBatch();
        super.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        flushBatch();
        super.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        flushBatch();
        super.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        flushBatch();
        super.processRecordAttributes(recordAttributes);
    }

    @Override
    public void processWatermark(WatermarkEvent watermark) throws Exception {
        flushBatch();
        super.processWatermark(watermark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        flushBatch();
    }

    @Override
    public void endInput() {
        flushBatch();
    }

    private void flushBatch() {
        if (records.isEmpty()) {
            return;
        }
        int logicalRecords = records.size();
        try (ArrowRowDataBatch batch = ArrowExchangeBatch.transpose(records, inputType, managedMemory.allocator())) {
            List<NativeExchangeFrame> frames =
                    router.route(serializedPlan, batch, managedMemory.allocator(), managedMemory);
            for (NativeExchangeFrame frame : frames) {
                output.collect(new StreamRecord<>(frame));
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), frames.size(), logicalRecords);
        }
        records.clear();
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
