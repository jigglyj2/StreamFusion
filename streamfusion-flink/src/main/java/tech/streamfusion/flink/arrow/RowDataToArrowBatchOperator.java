/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** The single RowData-to-Arrow conversion at a Flink source edge. */
final class RowDataToArrowBatchOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<RowData, ArrowRowDataBatch>, BoundedOneInput {
    static final int DEFAULT_BATCH_SIZE = 8192;

    private final RowType rowType;
    private final int[] fieldOrdinals;
    private final RowKind[] rowKinds = new RowKind[DEFAULT_BATCH_SIZE];
    private final boolean[] hasTimestamps = new boolean[DEFAULT_BATCH_SIZE];
    private final long[] timestamps = new long[DEFAULT_BATCH_SIZE];
    private int rowCount;
    private int batchSize;
    private transient FlinkManagedMemory managedMemory;
    private transient ArrowRowDataBatchWriter writer;

    RowDataToArrowBatchOperator(RowType rowType) {
        this(rowType, null);
    }

    RowDataToArrowBatchOperator(RowType rowType, int[] fieldOrdinals) {
        this.rowType = rowType;
        this.fieldOrdinals = fieldOrdinals == null ? null : fieldOrdinals.clone();
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-source-arrow-boundary");
        writer = ArrowRowDataBatchWriter.createAdaptive(rowType, managedMemory.allocator(), DEFAULT_BATCH_SIZE);
        batchSize = writer.batchCapacity();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData row = element.getValue();
        if (fieldOrdinals == null) {
            writer.write(row);
        } else {
            writer.write(row, fieldOrdinals);
        }
        rowKinds[rowCount] = row.getRowKind();
        hasTimestamps[rowCount] = element.hasTimestamp();
        timestamps[rowCount] = element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;
        rowCount++;
        if (rowCount == batchSize) {
            flush();
        }
    }

    @Override
    public void endInput() {
        flush();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        flush();
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        flush();
        super.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        flush();
        super.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        flush();
        super.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
        flush();
        super.processRecordAttributes(recordAttributes);
    }

    private void flush() {
        if (rowCount == 0) {
            return;
        }
        int logicalRows = rowCount;
        try (ArrowRowDataBatch batch = writer.finishBatch(rowKinds, hasTimestamps, timestamps)) {
            output.collect(new StreamRecord<>(batch));
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, logicalRows);
        } finally {
            writer.reset();
            rowCount = 0;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            if (managedMemory != null) {
                managedMemory.close();
                managedMemory = null;
            }
        } finally {
            super.close();
        }
    }
}
