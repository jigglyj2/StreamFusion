/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import javax.annotation.Nullable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Executes one native Arrow plan without exposing RowData at either operator edge. */
public final class StreamFusionArrowNativeOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final RowType outputType;
    private final byte[] serializedPlan;
    private final String memoryConsumerName;
    private final int nullMetricFieldIndex;
    private final @Nullable String nullMetricName;
    private final boolean preserveRecordTimestamps;
    private transient StreamFusionTaskMemory taskMemory;
    private transient Counter nullMetric;

    public StreamFusionArrowNativeOperator(RowType outputType, byte[] serializedPlan, String memoryConsumerName) {
        this(outputType, serializedPlan, memoryConsumerName, -1, null, true);
    }

    public StreamFusionArrowNativeOperator(
            RowType outputType,
            byte[] serializedPlan,
            String memoryConsumerName,
            int nullMetricFieldIndex,
            @Nullable String nullMetricName) {
        this(outputType, serializedPlan, memoryConsumerName, nullMetricFieldIndex, nullMetricName, true);
    }

    public StreamFusionArrowNativeOperator(
            RowType outputType,
            byte[] serializedPlan,
            String memoryConsumerName,
            int nullMetricFieldIndex,
            @Nullable String nullMetricName,
            boolean preserveRecordTimestamps) {
        this.outputType = outputType;
        this.serializedPlan = serializedPlan.clone();
        this.memoryConsumerName = memoryConsumerName;
        this.nullMetricFieldIndex = nullMetricFieldIndex;
        this.nullMetricName = nullMetricName;
        this.preserveRecordTimestamps = preserveRecordTimestamps;
    }

    @Override
    public void open() throws Exception {
        super.open();
        taskMemory = StreamFusionTaskMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                memoryConsumerName,
                serializedPlan);
        if (nullMetricName != null) {
            nullMetric = getMetricGroup().counter(nullMetricName);
        }
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) {
        ArrowRowDataBatch input = element.getValue();
        if (nullMetric != null) {
            nullMetric.inc(input.root().getVector(nullMetricFieldIndex).getNullCount());
        }
        try (NativeCalcResult result = ArrowCDataBridge.executeWithSelection(
                taskMemory.executionContext(), input, outputType, taskMemory.allocator())) {
            ArrowRowDataBatch outputBatch = result.selectEnvelopeFrom(input);
            if (!preserveRecordTimestamps) {
                outputBatch.withoutTimestamps();
            }
            output.collect(new StreamRecord<>(outputBatch));
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, outputBatch.size());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (taskMemory != null) {
                taskMemory.close();
                taskMemory = null;
            }
        } finally {
            super.close();
        }
    }
}
