/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.minibatch;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Row-time mini-batch watermark coalescing without changing the Arrow data plane. */
final class StreamFusionArrowRowTimeMiniBatchAssignerOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final long intervalMillis;
    private transient long currentWatermark;
    private transient long nextWatermark;

    StreamFusionArrowRowTimeMiniBatchAssignerOperator(long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    @Override
    public void open() throws Exception {
        super.open();
        currentWatermark = 0;
        nextWatermark = miniBatchStart(currentWatermark) + intervalMillis - 1;
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) {
        ArrowRowDataBatch batch = element.getValue();
        output.collect(element);
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
    }

    @Override
    public void processWatermark(Watermark watermark) {
        if (watermark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
            currentWatermark = Long.MAX_VALUE;
            output.emitWatermark(watermark);
            return;
        }
        currentWatermark = Math.max(currentWatermark, watermark.getTimestamp());
        if (currentWatermark >= nextWatermark) {
            advanceWatermark();
        }
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        advanceWatermark();
    }

    private void advanceWatermark() {
        output.emitWatermark(new Watermark(currentWatermark));
        long end = miniBatchStart(currentWatermark) + intervalMillis - 1;
        nextWatermark = end > currentWatermark ? end : end + intervalMillis;
    }

    private long miniBatchStart(long watermark) {
        return watermark - (watermark + intervalMillis) % intervalMillis;
    }
}
