/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowLocalWindowAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.nativebridge.NativeLocalWindowAggregateBridge;

/** State-free per-Arrow-batch slicing aggregation for Flink's local window stage. */
final class StreamFusionArrowLocalWindowAggregateOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final byte[] serializedPlan;
    private final RowType outputType;
    private final boolean inputChangelog;

    private transient FlinkManagedMemory managedMemory;
    private transient long nativeHandle;

    StreamFusionArrowLocalWindowAggregateOperator(byte[] serializedPlan, RowType outputType, boolean inputChangelog) {
        this.serializedPlan = serializedPlan.clone();
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-local-window-aggregate");
        nativeHandle = NativeLocalWindowAggregateBridge.create(serializedPlan, managedMemory);
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        if (!inputChangelog) {
            for (int row = 0; row < input.size(); row++) {
                if (input.rowKind(row) != RowKind.INSERT) {
                    throw new IllegalStateException(
                            "Native append-only local window aggregate got " + input.rowKind(row));
                }
            }
        }
        try (ArrowRowDataBatch result = ArrowLocalWindowAggregateCDataBridge.execute(
                nativeHandle, input, inputChangelog, outputType, managedMemory.allocator(), managedMemory)) {
            int physicalOutput = 0;
            if (result.size() > 0) {
                output.collect(new StreamRecord<>(result));
                physicalOutput = 1;
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            try {
                if (nativeHandle != 0) {
                    NativeLocalWindowAggregateBridge.destroy(nativeHandle);
                    nativeHandle = 0;
                }
            } finally {
                if (managedMemory != null) {
                    managedMemory.close();
                    managedMemory = null;
                }
            }
        } finally {
            super.close();
        }
    }
}
