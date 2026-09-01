/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Routes an existing Arrow batch by key group and emits Arrow IPC network frames. */
public final class NativeExchangeWriterOperator extends AbstractStreamOperator<NativeExchangeFrame>
        implements OneInputStreamOperator<ArrowRowDataBatch, NativeExchangeFrame> {
    private final RowType inputType;
    private final int[] keys;
    private final byte[] serializedPlan;
    private final NativeExchangeBatchRouter router;
    private transient FlinkManagedMemory managedMemory;
    private transient RowDataKeySelector keySelector;

    public NativeExchangeWriterOperator(RowType inputType, int[] keys, byte[] serializedPlan) {
        this(inputType, keys, serializedPlan, NativeExchangeBatchRouter.JNI);
    }

    public NativeExchangeWriterOperator(RowType inputType, byte[] serializedPlan) {
        this(inputType, new int[0], serializedPlan, NativeExchangeBatchRouter.JNI);
    }

    NativeExchangeWriterOperator(RowType inputType, byte[] serializedPlan, NativeExchangeBatchRouter router) {
        this(inputType, new int[0], serializedPlan, router);
    }

    NativeExchangeWriterOperator(
            RowType inputType, int[] keys, byte[] serializedPlan, NativeExchangeBatchRouter router) {
        this.inputType = inputType;
        this.keys = keys.clone();
        this.serializedPlan = serializedPlan.clone();
        this.router = router;
    }

    @Override
    public void open() throws Exception {
        super.open();
        managedMemory = FlinkManagedMemory.create(
                getContainingTask().getEnvironment(), getOperatorConfig(), getMetricGroup(), "streamfusion-exchange");
        if (NativeExchangePlanSerializer.requiresPreencodedKeys(inputType, keys)) {
            keySelector = KeySelectorUtil.getRowDataSelector(
                    getContainingTask().getUserCodeClassLoader(), keys, InternalTypeInfo.of(inputType));
        }
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try (ArrowExchangeBatch.EnvelopeBatch envelope =
                ArrowExchangeBatch.withEnvelope(input, inputType, preencodedKeys(input))) {
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

    private List<byte[]> preencodedKeys(ArrowRowDataBatch input) throws Exception {
        if (keySelector == null) {
            return null;
        }
        List<byte[]> encoded = new java.util.ArrayList<>(input.size());
        for (int row = 0; row < input.size(); row++) {
            RowData selected = keySelector.getKey(input.rowView(row));
            if (!(selected instanceof BinaryRowData)) {
                throw new IllegalStateException("Native exchange requires Flink's BinaryRowData key selector");
            }
            BinaryRowData binary = (BinaryRowData) selected;
            encoded.add(
                    BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        }
        return encoded;
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
