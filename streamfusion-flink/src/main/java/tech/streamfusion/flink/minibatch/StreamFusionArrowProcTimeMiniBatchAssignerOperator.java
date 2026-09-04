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
        long now = getProcessingTimeService().getCurrentProcessingTime();
        advance(now);
        ArrowRowDataBatch batch = element.getValue();
        output.collect(element);
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        advance(now);
        getProcessingTimeService().registerTimer(currentBatch + intervalMillis, this);
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
