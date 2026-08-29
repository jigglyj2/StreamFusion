/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.changelog;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Drops UPDATE_BEFORE envelope entries without materializing the Arrow payload as rows. */
final class StreamFusionDropUpdateBeforeOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private transient FlinkManagedMemory managedMemory;

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-drop-update-before");
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) {
        ArrowRowDataBatch batch = element.getValue();
        int emitted = 0;
        int emittedBatches = 0;
        for (int[] run : retainedRuns(batch)) {
            int length = run[1];
            emitted += length;
            emittedBatches++;
            emitRun(element, batch, run[0], length);
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), emittedBatches, emitted);
    }

    static List<int[]> retainedRuns(ArrowRowDataBatch batch) {
        List<int[]> runs = new ArrayList<>();
        int runStart = -1;
        for (int row = 0; row <= batch.size(); row++) {
            boolean keep = row < batch.size() && batch.rowKind(row) != RowKind.UPDATE_BEFORE;
            if (keep && runStart < 0) {
                runStart = row;
            } else if (!keep && runStart >= 0) {
                runs.add(new int[] {runStart, row - runStart});
                runStart = -1;
            }
        }
        return runs;
    }

    private void emitRun(StreamRecord<ArrowRowDataBatch> element, ArrowRowDataBatch batch, int offset, int length) {
        if (offset == 0 && length == batch.size()) {
            output.collect(element);
            return;
        }
        try (ArrowRowDataBatch selected = batch.slice(offset, length, managedMemory.allocator())) {
            output.collect(new StreamRecord<>(selected));
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
