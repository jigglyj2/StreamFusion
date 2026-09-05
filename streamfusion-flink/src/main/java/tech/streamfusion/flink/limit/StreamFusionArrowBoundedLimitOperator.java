/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.limit;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Vectorized bounded LIMIT/OFFSET preserving Flink's physical-record order and changelog kinds. */
final class StreamFusionArrowBoundedLimitOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final long limitStart;
    private final long limitEnd;
    private long seen;

    StreamFusionArrowBoundedLimitOperator(boolean global, long limitStart, long limitEnd) {
        this.limitStart = global ? limitStart : 0;
        this.limitEnd = limitEnd;
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch batch = element.getValue();
        long batchStart = seen;
        seen = seen > Long.MAX_VALUE - batch.size() ? Long.MAX_VALUE : seen + batch.size();
        long selectedStart = Math.max(batchStart, limitStart);
        long selectedEnd = Math.min(seen, limitEnd);
        int outputRows = selectedEnd > selectedStart ? Math.toIntExact(selectedEnd - selectedStart) : 0;
        int physicalOutput = 0;
        if (outputRows == batch.size()) {
            output.collect(element);
            physicalOutput = 1;
        } else if (outputRows > 0) {
            int offset = Math.toIntExact(selectedStart - batchStart);
            try (ArrowRowDataBatch selected = batch.slice(offset, outputRows)) {
                output.collect(new StreamRecord<>(selected));
            }
            physicalOutput = 1;
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, outputRows);
    }
}
