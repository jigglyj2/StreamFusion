/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** The single Arrow-to-RowData view boundary immediately before a Flink sink. */
final class ArrowBatchToRowDataOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<ArrowRowDataBatch, RowData> {
    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) {
        ArrowRowDataBatch batch = element.getValue();
        for (int row = 0; row < batch.size(); row++) {
            RowData view = batch.rowView(row);
            view.setRowKind(batch.rowKind(row));
            output.collect(
                    batch.hasTimestamp(row)
                            ? new StreamRecord<>(view, batch.timestamp(row))
                            : new StreamRecord<>(view));
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
    }
}
