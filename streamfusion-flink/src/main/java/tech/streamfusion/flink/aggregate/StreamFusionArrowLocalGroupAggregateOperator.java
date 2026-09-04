/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowLocalGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.FlinkBinaryRowKeyEncoder;
import tech.streamfusion.nativebridge.NativeLocalGroupAggregateBridge;

/** Native, state-free local half of Flink's local/global mini-batch aggregate pair. */
final class StreamFusionArrowLocalGroupAggregateOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final byte[] serializedPlan;
    private final RowType outputType;
    private final boolean inputChangelog;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;

    private transient FlinkManagedMemory managedMemory;
    private transient long nativeHandle;

    StreamFusionArrowLocalGroupAggregateOperator(
            byte[] serializedPlan,
            RowType inputType,
            RowType outputType,
            int[] grouping,
            boolean inputChangelog,
            RowDataKeySelector keySelector) {
        this.serializedPlan = serializedPlan.clone();
        this.outputType = outputType;
        this.inputChangelog = inputChangelog;
        this.preencodeKeys = FlinkBinaryRowKeyEncoder.requiresPreencoding(inputType, grouping);
        this.keySelector = keySelector;
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-local-group-aggregate");
        nativeHandle = NativeLocalGroupAggregateBridge.create(serializedPlan, managedMemory);
        getRuntimeContext()
                .getMetricGroup()
                .gauge(
                        "bundleSize",
                        () -> Math.toIntExact(NativeLocalGroupAggregateBridge.pendingElementCount(nativeHandle)));
        getRuntimeContext().getMetricGroup().gauge("bundleRatio", () -> {
            long keys = NativeLocalGroupAggregateBridge.pendingKeyCount(nativeHandle);
            return keys == 0 ? 0.0 : 1.0 * NativeLocalGroupAggregateBridge.pendingElementCount(nativeHandle) / keys;
        });
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        if (!inputChangelog) {
            for (int row = 0; row < input.size(); row++) {
                if (input.rowKind(row) != RowKind.INSERT) {
                    throw new IllegalStateException(
                            "Native append-only local group aggregate got " + input.rowKind(row));
                }
            }
        }
        List<byte[]> keys =
                preencodeKeys ? FlinkBinaryRowKeyEncoder.encode(input, keySelector, "local group aggregate") : null;
        try (ArrowRowDataBatch outputBatch = ArrowLocalGroupAggregateCDataBridge.execute(
                nativeHandle, input, keys, inputChangelog, outputType, managedMemory.allocator(), managedMemory)) {
            emit(outputBatch);
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        flushBundle();
        super.processWatermark(watermark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flushBundle();
    }

    @Override
    public void endInput() throws Exception {
        flushBundle();
    }

    @Override
    public void finish() throws Exception {
        flushBundle();
        super.finish();
    }

    private void flushBundle() throws Exception {
        if (nativeHandle == 0 || NativeLocalGroupAggregateBridge.pendingElementCount(nativeHandle) == 0) {
            return;
        }
        try (ArrowRowDataBatch outputBatch = ArrowLocalGroupAggregateCDataBridge.finishBundle(
                nativeHandle, outputType, managedMemory.allocator(), managedMemory)) {
            emit(outputBatch);
        }
    }

    private void emit(ArrowRowDataBatch outputBatch) {
        int physical = 0;
        if (outputBatch.size() > 0) {
            output.collect(new StreamRecord<>(outputBatch));
            physical = 1;
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physical, outputBatch.size());
    }

    @Override
    public void close() throws Exception {
        try {
            try {
                if (nativeHandle != 0) {
                    NativeLocalGroupAggregateBridge.destroy(nativeHandle);
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
