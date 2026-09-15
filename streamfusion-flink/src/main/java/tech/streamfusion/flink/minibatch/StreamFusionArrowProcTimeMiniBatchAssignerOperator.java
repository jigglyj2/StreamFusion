/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.minibatch;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Processing-time mini-batch boundary assignment without changing the Arrow data plane. */
final class StreamFusionArrowProcTimeMiniBatchAssignerOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>,
                ProcessingTimeService.ProcessingTimeCallback {
    private final long intervalMillis;
    private transient long currentBatch;

    StreamFusionArrowProcTimeMiniBatchAssignerOperator(long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    @Override
    public void open() throws Exception {
        super.open();
        currentBatch = 0;
        long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + intervalMillis, this);
        getRuntimeContext().getMetricGroup().gauge("currentBatch", (Gauge<Long>) () -> currentBatch);
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch batch = element.getValue();
        int rangeStart = 0;
        int emittedBatches = 0;
        for (int row = 0; row < batch.size(); row++) {
            long now = getProcessingTimeService().getCurrentProcessingTime();
            long boundary = now - now % intervalMillis;
            if (boundary > currentBatch) {
                // Flink emits the marker before this logical record. Complete the preceding
                // Arrow range first so downstream bundle drains see the same input prefix.
                if (rangeStart < row) {
                    emitRange(element, rangeStart, row - rangeStart);
                    emittedBatches++;
                }
                rangeStart = row;
                advance(now);
            }
        }
        if (rangeStart < batch.size()) {
            emitRange(element, rangeStart, batch.size() - rangeStart);
            emittedBatches++;
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), emittedBatches, batch.size());
    }

    private void emitRange(StreamRecord<ArrowRowDataBatch> element, int offset, int length) {
        ArrowRowDataBatch batch = element.getValue();
        if (offset == 0 && length == batch.size()) {
            output.collect(element);
            return;
        }
        try (ArrowRowDataBatch selected = batch.slice(offset, length)) {
            output.collect(element.copy(selected));
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        advance(now);
        // The emitted watermark may already be MAX_WATERMARK. Flink schedules
        // from the clock interval even when a queued callback runs after that watermark.
        long clockBatch = now - now % intervalMillis;
        getProcessingTimeService().registerTimer(clockBatch + intervalMillis, this);
    }

    private void advance(long now) {
        long batch = now - now % intervalMillis;
        if (batch > currentBatch) {
            currentBatch = batch;
            output.emitWatermark(new Watermark(batch));
        }
    }

    @Override
    public void processWatermark(Watermark watermark) {
        if (watermark.getTimestamp() == Long.MAX_VALUE && currentBatch != Long.MAX_VALUE) {
            currentBatch = Long.MAX_VALUE;
            output.emitWatermark(watermark);
        }
    }
}
