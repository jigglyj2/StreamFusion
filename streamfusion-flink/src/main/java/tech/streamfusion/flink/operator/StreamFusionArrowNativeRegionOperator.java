/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.metrics.StreamFusionNativeMetricTree;

/** One Flink lifecycle owner for a native tree; no per-operator or operator-pair execution driver. */
public final class StreamFusionArrowNativeRegionOperator extends AbstractStreamOperatorV2<ArrowRowDataBatch>
        implements MultipleInputStreamOperator<ArrowRowDataBatch>, BoundedMultiInput {
    private final Environment environment;
    private final int subtaskIndex;
    private final List<RowType> inputTypes;
    private final RowType outputType;
    private final byte[] plan;
    private final List<Long> stateIds;
    private tech.streamfusion.flink.state.NativeRegionStateLifecycle stateLifecycle;
    private final List<Input> inputs;
    private final boolean[] ended;
    private final List<byte[]> exchangePlans;
    private StreamFusionTaskMemory memory;
    private ArrowNativePlanDispatcher dispatcher;
    private StreamFusionNativeMetricTree metricTree;
    private NativeRegionControlScheduler controls;

    StreamFusionArrowNativeRegionOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters,
            List<RowType> inputTypes,
            RowType outputType,
            byte[] plan,
            List<Long> stateIds,
            List<byte[]> exchangePlans) {
        super(parameters, inputTypes.size());
        environment = parameters.getContainingTask().getEnvironment();
        subtaskIndex = parameters.getContainingTask().getIndexInSubtaskGroup();
        this.inputTypes = List.copyOf(inputTypes);
        this.outputType = outputType;
        this.plan = plan.clone();
        this.stateIds = List.copyOf(stateIds);
        ended = new boolean[inputTypes.size()];
        List<Input> ports = new ArrayList<>();
        this.exchangePlans = exchangePlans.stream().map(byte[]::clone).collect(java.util.stream.Collectors.toList());
        for (int index = 0; index < inputTypes.size(); index++) {
            ports.add(new RegionInput(index + 1));
        }
        inputs = List.copyOf(ports);
    }

    @Override
    public void initializeState(org.apache.flink.runtime.state.StateInitializationContext context) throws Exception {
        if (stateIds.isEmpty()) {
            super.initializeState(context);
        } else {
            stateLifecycle = new tech.streamfusion.flink.state.NativeRegionStateLifecycle();
            stateLifecycle.initialize(
                    context,
                    environment,
                    config,
                    getMetricGroup(),
                    getKeyedStateBackend(),
                    getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks(),
                    plan,
                    stateIds);
            memory = stateLifecycle.memory();
        }
    }

    @Override
    protected boolean isUsingCustomRawKeyedState() {
        return !stateIds.isEmpty();
    }

    @Override
    public org.apache.flink.streaming.api.operators.OperatorSnapshotFutures snapshotState(
            long id,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory factory)
            throws Exception {
        if (stateLifecycle == null) return super.snapshotState(id, timestamp, options, factory);
        stateLifecycle.beginSnapshot(options);
        try {
            return super.snapshotState(id, timestamp, options, factory);
        } finally {
            stateLifecycle.finishSnapshot();
        }
    }

    @Override
    public void snapshotState(org.apache.flink.runtime.state.StateSnapshotContext context) throws Exception {
        if (stateLifecycle == null) super.snapshotState(context);
        else stateLifecycle.writeSnapshot(context);
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (!stateIds.isEmpty() && memory == null) {
            throw new IllegalStateException("Native region state initialization did not complete");
        }
        if (memory == null)
            memory = StreamFusionTaskMemory.create(
                    environment, config, getMetricGroup(), "streamfusion-native-region", plan);
        dispatcher =
                new ArrowNativePlanDispatcher(memory.executionContext(), inputTypes, outputType, memory.allocator());
        metricTree = StreamFusionNativeMetricTree.forRegion(
                memory.executionContext().identifiedPlan(),
                getOperatorID(),
                environment.getMetricGroup(),
                environment.getTaskManagerInfo().getConfiguration(),
                subtaskIndex);
        metricTree.bindGauges(memory.executionContext().gaugeSchema(), memory.executionContext()::gaugeSnapshot);
        controls = new NativeRegionControlScheduler(
                memory.executionContext().identifiedPlan(),
                inputTypes.size(),
                memory.executionContext().controlCapabilities(),
                stateLifecycle == null ? java.util.Map.of() : stateLifecycle.restoredWindowWatermarks(),
                this::dispatchControl,
                new NativeRegionControlTree.Listener() {
                    @Override
                    public void inputWatermark(long nodeId, int port, long timestamp) {
                        metricTree.inputWatermark(nodeId, port, timestamp);
                    }

                    @Override
                    public void watermark(long nodeId, long timestamp) throws Exception {
                        if (stateLifecycle != null) stateLifecycle.watermark(nodeId, timestamp);
                        metricTree.watermark(nodeId, timestamp);
                        if (nodeId == controls.rootId()) {
                            processWatermark(new Watermark(timestamp));
                        }
                    }

                    @Override
                    public void status(long nodeId, WatermarkStatus status) {
                        if (nodeId == controls.rootId()) {
                            output.emitWatermarkStatus(status);
                        }
                    }

                    @Override
                    public void latency(long nodeId, LatencyMarker marker) {
                        metricTree.latency(nodeId, marker);
                        if (nodeId == controls.rootId()) {
                            reportOrForwardLatencyMarker(marker);
                        }
                    }
                });
    }

    @Override
    public List<Input> getInputs() {
        return inputs;
    }

    @Override
    protected void reportWatermark(Watermark watermark, int inputId) throws Exception {
        controls.watermark(inputId - 1, watermark.getTimestamp());
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status, int inputId) throws Exception {
        controls.status(inputId - 1, status);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        int port = java.util.Objects.checkIndex(inputId - 1, ended.length);
        if (!ended[port]) {
            controls.endInput(port);
            ended[port] = true;
        }
    }

    @Override
    public void finish() throws Exception {
        controls.finish();
        java.util.Arrays.fill(ended, true);
        super.finish();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        controls.beforeCheckpoint(checkpointId);
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    private void dispatchControl(byte[] request) {
        try {
            dispatcher.control(request, this::emitOutput);
        } catch (RuntimeException | Error failure) {
            metricTree.updateAfterFailure(memory.executionContext(), failure);
            throw failure;
        }
        metricTree.update(memory.executionContext());
    }

    private void emitOutput(ArrowRowDataBatch batch) {
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
        output.collect(new StreamRecord<>(batch));
    }

    private void process(int port, ArrowRowDataBatch input) {
        controls.requireHealthy();
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
        try {
            dispatcher.process(port, input, this::emitOutput);
        } catch (RuntimeException | Error failure) {
            metricTree.updateAfterFailure(memory.executionContext(), failure);
            throw failure;
        }
        metricTree.update(memory.executionContext());
    }

    @Override
    public void close() throws Exception {
        try {
            org.apache.flink.util.IOUtils.closeAll(
                    dispatcher, metricTree, stateLifecycle == null ? memory : stateLifecycle);
        } finally {
            dispatcher = null;
            metricTree = null;
            memory = null;
            stateLifecycle = null;
            super.close();
        }
    }

    private final class RegionInput extends AbstractInput<Object, ArrowRowDataBatch> {
        private RegionInput(int inputId) {
            super(StreamFusionArrowNativeRegionOperator.this, inputId);
        }

        @Override
        public void processElement(StreamRecord<Object> record) {
            int port = inputId - 1;
            if (ended[port]) {
                throw new IllegalStateException("Native region received data after input " + inputId + " ended");
            }
            Object value = record.getValue();
            if (value instanceof ArrowRowDataBatch) {
                process(port, (ArrowRowDataBatch) value);
            } else if (value instanceof NativeExchangeFrame) {
                controls.requireHealthy();
                try {
                    dispatcher.processFrame(
                            port,
                            exchangePlans.get(port),
                            (NativeExchangeFrame) value,
                            rows -> FlinkMetricParity.replacePhysicalRecords(
                                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, rows),
                            StreamFusionArrowNativeRegionOperator.this::emitOutput);
                } catch (RuntimeException | Error failure) {
                    metricTree.updateAfterFailure(memory.executionContext(), failure);
                    throw failure;
                }
                metricTree.update(memory.executionContext());
            } else {
                throw new IllegalArgumentException("Native region input must be Arrow or an exchange-edge IPC frame");
            }
        }

        @Override
        public void processLatencyMarker(LatencyMarker marker) throws Exception {
            controls.latency(inputId - 1, marker);
        }
    }
}
