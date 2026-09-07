/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import javax.annotation.Nullable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.metrics.StreamFusionNativeMetricTree;

/** Executes one native Arrow plan without exposing RowData at either operator edge. */
public final class StreamFusionArrowNativeOperator extends AbstractStreamOperator<ArrowRowDataBatch>
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final @Nullable RowType plannedInputType;
    private final RowType outputType;
    private final byte[] serializedPlan;
    private final String memoryConsumerName;
    private final int nullMetricFieldIndex;
    private final @Nullable String nullMetricName;
    private final boolean preserveRecordTimestamps;
    private final boolean separateMetricOwner;
    private transient StreamFusionTaskMemory taskMemory;
    private transient ArrowNativePlanDispatcher nativeExecution;
    private transient Counter nullMetric;
    private transient long lastNativeNullMetric;
    private transient StreamFusionNativeMetricTree metricTree;
    private transient long lastOwnerInputRecords;
    private transient NativeRegionControlScheduler controls;
    private transient boolean ended;

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
        this(
                outputType,
                serializedPlan,
                memoryConsumerName,
                nullMetricFieldIndex,
                nullMetricName,
                preserveRecordTimestamps,
                false,
                null);
    }

    public static StreamFusionArrowNativeOperator forRegion(
            RowType inputType, RowType outputType, byte[] plan, String memoryConsumerName) {
        return new StreamFusionArrowNativeOperator(
                outputType,
                plan,
                memoryConsumerName,
                -1,
                null,
                true,
                true,
                java.util.Objects.requireNonNull(inputType));
    }

    private StreamFusionArrowNativeOperator(
            RowType outputType,
            byte[] serializedPlan,
            String memoryConsumerName,
            int nullMetricFieldIndex,
            @Nullable String nullMetricName,
            boolean preserveRecordTimestamps,
            boolean separateMetricOwner,
            @Nullable RowType plannedInputType) {
        this.plannedInputType = plannedInputType;
        this.outputType = outputType;
        this.serializedPlan = serializedPlan.clone();
        this.memoryConsumerName = memoryConsumerName;
        this.nullMetricFieldIndex = nullMetricFieldIndex;
        this.nullMetricName = nullMetricName;
        this.preserveRecordTimestamps = preserveRecordTimestamps;
        this.separateMetricOwner = separateMetricOwner;
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
        metricTree = separateMetricOwner
                ? StreamFusionNativeMetricTree.forRegion(
                        taskMemory.executionContext().identifiedPlan(),
                        getOperatorID(),
                        getContainingTask().getEnvironment().getMetricGroup(),
                        getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration(),
                        getContainingTask().getIndexInSubtaskGroup())
                : new StreamFusionNativeMetricTree(
                        taskMemory.executionContext().identifiedPlan(),
                        getOperatorID(),
                        getContainingTask().getEnvironment().getMetricGroup(),
                        getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration(),
                        getContainingTask().getIndexInSubtaskGroup());
        if (nullMetricName != null) {
            nullMetric = getMetricGroup().counter(nullMetricName);
        }
        if (separateMetricOwner) {
            nativeExecution = new ArrowNativePlanDispatcher(
                    taskMemory.executionContext(),
                    java.util.List.of(plannedInputType),
                    outputType,
                    taskMemory.allocator());
            metricTree.bindGauges(
                    taskMemory.executionContext().gaugeSchema(), taskMemory.executionContext()::gaugeSnapshot);
            controls = new NativeRegionControlScheduler(
                    taskMemory.executionContext().identifiedPlan(),
                    1,
                    taskMemory.executionContext().controlCapabilities(),
                    this::dispatchControl,
                    new NativeRegionControlTree.Listener() {
                        @Override
                        public void inputWatermark(long id, int port, long timestamp) {
                            metricTree.inputWatermark(id, port, timestamp);
                        }

                        @Override
                        public void watermark(long id, long timestamp) throws Exception {
                            metricTree.watermark(id, timestamp);
                            if (id == controls.rootId()) forwardWatermark(new Watermark(timestamp));
                        }

                        @Override
                        public void status(long id, WatermarkStatus status) throws Exception {
                            if (id == controls.rootId()) forwardStatus(status);
                        }

                        @Override
                        public void latency(long id, LatencyMarker marker) throws Exception {
                            metricTree.latency(id, marker);
                            if (id == controls.rootId()) forwardLatency(marker);
                        }
                    });
        }
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) {
        if (ended) throw new IllegalStateException("Native region received data after input ended");
        if (controls != null) controls.requireHealthy();
        ArrowRowDataBatch input = element.getValue();
        if (separateMetricOwner)
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
        if (nullMetric != null && nullMetricFieldIndex >= 0) {
            nullMetric.inc(input.root().getVector(nullMetricFieldIndex).getNullCount());
        }
        try {
            if (nativeExecution == null) {
                nativeExecution = new ArrowNativePlanDispatcher(
                        taskMemory.executionContext(),
                        java.util.List.of(input.rowType()),
                        outputType,
                        taskMemory.allocator());
            }
            nativeExecution.process(0, input, this::emitOutput);
        } catch (RuntimeException | Error failure) {
            metricTree.updateAfterFailure(taskMemory.executionContext(), failure);
            throw failure;
        }
        tech.streamfusion.nativebridge.NativeCalcBridge.recordFusedBatches(1);
        propagateNativeMetrics();
        if (!separateMetricOwner) {
            long ownerInput = metricTree.ownerInputRecords();
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(),
                    1,
                    ownerInput - lastOwnerInputRecords);
            lastOwnerInputRecords = ownerInput;
        }
    }

    private void propagateNativeMetrics() {
        metricTree.update(taskMemory.executionContext());
        if (nullMetric == null || nullMetricFieldIndex >= 0) {
            return;
        }
        long current = taskMemory.executionContext().metricValue(nullMetricName);
        nullMetric.inc(current - lastNativeNullMetric);
        lastNativeNullMetric = current;
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (controls != null) controls.watermark(0, mark.getTimestamp());
        else {
            metricTree.watermark(mark.getTimestamp());
            forwardWatermark(mark);
        }
    }

    @Override
    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        if (controls != null) controls.latency(0, marker);
        else {
            metricTree.latency(marker);
            forwardLatency(marker);
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        if (controls != null) controls.status(0, status);
        else forwardStatus(status);
    }

    private void forwardWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
    }

    private void forwardStatus(WatermarkStatus status) throws Exception {
        super.processWatermarkStatus(status);
    }

    private void forwardLatency(LatencyMarker marker) throws Exception {
        super.processLatencyMarker(marker);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (controls != null) controls.beforeCheckpoint(checkpointId);
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void endInput() throws Exception {
        if (!ended) {
            if (controls != null) controls.endInput(0);
            ended = true;
        }
    }

    @Override
    public void finish() throws Exception {
        if (controls != null) controls.finish();
        ended = true;
        super.finish();
    }

    private void dispatchControl(byte[] request) {
        try {
            nativeExecution.control(request, this::emitOutput);
        } catch (RuntimeException | Error failure) {
            metricTree.updateAfterFailure(taskMemory.executionContext(), failure);
            throw failure;
        }
        propagateNativeMetrics();
    }

    private void emitOutput(ArrowRowDataBatch outputBatch) {
        if (!preserveRecordTimestamps) {
            outputBatch.withoutTimestamps();
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, outputBatch.size());
        output.collect(new StreamRecord<>(outputBatch));
    }

    @Override
    public void close() throws Exception {
        try {
            if (metricTree != null) {
                metricTree.close();
                metricTree = null;
            }
            if (taskMemory != null) {
                try {
                    if (nativeExecution != null) {
                        nativeExecution.close();
                        nativeExecution = null;
                    }
                } finally {
                    taskMemory.close();
                    taskMemory = null;
                }
            }
        } finally {
            super.close();
        }
    }
}
