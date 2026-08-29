/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Routes an existing Arrow batch by key group and emits Arrow IPC network frames. */
public final class NativeExchangeWriterOperator extends AbstractStreamOperator<NativeExchangeFrame>
        implements OneInputStreamOperator<ArrowRowDataBatch, NativeExchangeFrame> {
    private final RowType inputType;
    private final byte[] serializedPlan;
    private final NativeExchangeBatchRouter router;
    private transient FlinkManagedMemory managedMemory;

    public NativeExchangeWriterOperator(RowType inputType, byte[] serializedPlan) {
        this(inputType, serializedPlan, NativeExchangeBatchRouter.JNI);
    }

    NativeExchangeWriterOperator(RowType inputType, byte[] serializedPlan, NativeExchangeBatchRouter router) {
        this.inputType = inputType;
        this.serializedPlan = serializedPlan.clone();
        this.router = router;
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(), getOperatorConfig(), getMetricGroup(), "streamfusion-exchange");
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) {
        ArrowRowDataBatch input = element.getValue();
        try (ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, inputType)) {
            List<NativeExchangeFrame> frames =
                    router.route(serializedPlan, envelope.batch(), managedMemory.allocator(), managedMemory);
            for (NativeExchangeFrame frame : frames) {
                output.collect(new StreamRecord<>(frame));
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), frames.size(), input.size());
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
