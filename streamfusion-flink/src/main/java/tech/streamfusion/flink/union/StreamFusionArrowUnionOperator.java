/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.union;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.nativebridge.NativeUnionBridge;

/** Arrow-native UNION ALL; arrival order and control-input coordination remain with Flink. */
final class StreamFusionArrowUnionOperator extends AbstractStreamOperatorV2<ArrowRowDataBatch>
        implements MultipleInputStreamOperator<ArrowRowDataBatch>, BoundedMultiInput {
    private final int inputCount;
    private final @Nullable RowType rowType;
    private final @Nullable byte[] exchangePlan;
    private transient Environment taskEnvironment;
    private transient FlinkManagedMemory managedMemory;

    StreamFusionArrowUnionOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters,
            int inputCount,
            @Nullable RowType rowType,
            @Nullable byte[] exchangePlan) {
        super(parameters, inputCount);
        if (inputCount < 2) {
            throw new IllegalArgumentException("StreamFusion UNION ALL requires at least two inputs");
        }
        this.inputCount = inputCount;
        this.rowType = rowType;
        this.exchangePlan = exchangePlan == null ? null : exchangePlan.clone();
        this.taskEnvironment = parameters.getContainingTask().getEnvironment();
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (exchangePlan != null) {
            managedMemory = FlinkManagedMemory.create(
                    taskEnvironment, getOperatorConfig(), getMetricGroup(), "streamfusion-union-exchange");
        }
    }

    @Override
    public List<Input> getInputs() {
        List<Input> inputs = new ArrayList<>(inputCount);
        for (int input = 1; input <= inputCount; input++) {
            inputs.add(new UnionInput(this, input));
        }
        return inputs;
    }

    @Override
    public void endInput(int inputId) {}

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

    private final class UnionInput extends AbstractInput<Object, ArrowRowDataBatch> {
        private UnionInput(AbstractStreamOperatorV2<ArrowRowDataBatch> owner, int inputId) {
            super(owner, inputId);
        }

        @Override
        public void processElement(StreamRecord<Object> element) {
            Object value = element.getValue();
            if (value instanceof ArrowRowDataBatch) {
                forward((ArrowRowDataBatch) value, element);
                return;
            }
            if (!(value instanceof NativeExchangeFrame)
                    || rowType == null
                    || exchangePlan == null
                    || managedMemory == null) {
                throw new IllegalStateException("StreamFusion UNION received an invalid network input");
            }
            try (ArrowExchangeInputBatch decoded = ArrowExchangeInputCDataBridge.decode(
                    exchangePlan, (NativeExchangeFrame) value, rowType, managedMemory.allocator(), managedMemory)) {
                forward(decoded.arrowBatch(), null);
            }
        }

        private void forward(ArrowRowDataBatch batch, @Nullable StreamRecord<Object> original) {
            output.collect(
                    original == null || !original.hasTimestamp()
                            ? new StreamRecord<>(batch)
                            : new StreamRecord<>(batch, original.getTimestamp()));
            NativeUnionBridge.recordForwardedBatch();
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
        }
    }
}
