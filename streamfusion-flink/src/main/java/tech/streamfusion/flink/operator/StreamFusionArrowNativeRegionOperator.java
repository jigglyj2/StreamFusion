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
import tech.streamfusion.flink.arrow.ArrowNativeRegionDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.metrics.StreamFusionNativeMetricTree;

/** One Flink lifecycle owner for a native tree or shared region; no per-operator or operator-pair execution driver. */
public final class StreamFusionArrowNativeRegionOperator extends AbstractStreamOperatorV2<ArrowRowDataBatch>
        implements MultipleInputStreamOperator<ArrowRowDataBatch>,
                BoundedMultiInput,
                org.apache.flink.streaming.api.operators.OneInputStreamOperator<Object, ArrowRowDataBatch> {
    private final Environment environment;
    private final int subtaskIndex;
    private final List<RowType> inputTypes;
    private final List<RowType> outputTypes;
    private final tech.streamfusion.proto.plan.v1.NativeRegionPlan sharedPlan;
    private final NativeSharedRegionOutputs sharedOutputs;
    private final byte[] plan;
    private final List<Long> stateIds;
    private tech.streamfusion.flink.state.NativeRegionStateLifecycle stateLifecycle;
    private final List<Input> inputs;
    private final boolean[] ended;
    private final List<byte[]> exchangePlans;
    private final tech.streamfusion.flink.window.NativeLocalWindowResources localWindowResources;
    private StreamFusionTaskMemory memory;
    private ArrowNativeRegionDispatcher dispatcher;
    private StreamFusionNativeMetricTree metricTree;
    private NativeRegionControlScheduler controls;
    private NativeRegionProcessingTimeScheduler processingTimers;

    StreamFusionArrowNativeRegionOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters,
            List<RowType> inputTypes,
            List<RowType> outputTypes,
            byte[] plan,
            List<Long> stateIds,
            List<byte[]> exchangePlans,
            tech.streamfusion.flink.window.NativeLocalWindowResources localWindowResources,
            boolean sharedRegion) {
        super(parameters, inputTypes.size());
        environment = parameters.getContainingTask().getEnvironment();
        subtaskIndex = parameters.getContainingTask().getIndexInSubtaskGroup();
        this.inputTypes = List.copyOf(inputTypes);
        this.outputTypes = List.copyOf(outputTypes);
        if (sharedRegion) {
            try {
                sharedPlan = tech.streamfusion.proto.plan.v1.NativeRegionPlan.parseFrom(plan);
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Invalid shared native region", failure);
            }
            sharedOutputs = new NativeSharedRegionOutputs(sharedPlan, outputTypes.size());
            long latency = getExecutionConfig().isLatencyTrackingConfigured()
                    ? getExecutionConfig().getLatencyTrackingInterval()
                    : environment
                            .getTaskManagerInfo()
                            .getConfiguration()
                            .get(org.apache.flink.configuration.MetricOptions.LATENCY_INTERVAL)
                            .toMillis();
            if (latency > 0)
                throw new IllegalArgumentException(
                        "Shared native regions do not yet support Flink sampled latency routing");
        } else {
            sharedPlan = null;
            sharedOutputs = null;
        }
        this.plan = plan.clone();
        this.localWindowResources = localWindowResources;
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
            if (sharedPlan != null) {
                stateLifecycle.initializeRegion(
                        context,
                        environment,
                        config,
                        getMetricGroup(),
                        getKeyedStateBackend(),
                        getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks(),
                        plan,
                        stateIds,
                        localWindowResources.resolve(environment, config));
            } else {
                stateLifecycle.initialize(
                        context,
                        environment,
                        config,
                        getMetricGroup(),
                        getKeyedStateBackend(),
                        getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks(),
                        plan,
                        stateIds,
                        localWindowResources.resolve(environment, config));
            }
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
        if (memory == null) {
            byte[] resources = localWindowResources.resolve(environment, config);
            memory = sharedPlan == null
                    ? StreamFusionTaskMemory.createWithState(
                            environment,
                            config,
                            getMetricGroup(),
                            "streamfusion-native-region",
                            plan,
                            ignored -> null,
                            resources)
                    : StreamFusionTaskMemory.createRegionWithState(
                            environment,
                            config,
                            getMetricGroup(),
                            "streamfusion-native-region",
                            plan,
                            ignored -> null,
                            resources);
        }
        metricTree = sharedPlan == null
                ? StreamFusionNativeMetricTree.forRegion(
                        memory.executionContext().identifiedPlan(),
                        getOperatorID(),
                        environment.getMetricGroup(),
                        environment.getTaskManagerInfo().getConfiguration(),
                        subtaskIndex)
                : StreamFusionNativeMetricTree.forSharedRegion(
                        sharedPlan,
                        getOperatorID(),
                        environment.getMetricGroup(),
                        environment.getTaskManagerInfo().getConfiguration(),
                        subtaskIndex);
        metricTree.bindGauges(
                memory.executionContext().gaugeSchema(),
                memory.executionContext()::gaugeSnapshot,
                getProcessingTimeService()::getCurrentProcessingTime);
        var listener = new NativeRegionControlTree.Listener() {
            @Override
            public void inputWatermark(long nodeId, int port, long timestamp) {
                metricTree.inputWatermark(nodeId, port, timestamp);
            }

            @Override
            public void watermark(long nodeId, long timestamp) throws Exception {
                if (stateLifecycle != null) stateLifecycle.watermark(nodeId, timestamp);
                metricTree.watermark(nodeId, timestamp);
                if (sharedOutputs != null ? sharedOutputs.watermark(nodeId, timestamp) : nodeId == controls.rootId())
                    StreamFusionArrowNativeRegionOperator.super.processWatermark(new Watermark(timestamp));
            }

            @Override
            public void status(long nodeId, WatermarkStatus status) {
                if (sharedOutputs != null ? sharedOutputs.status(nodeId, status) : nodeId == controls.rootId())
                    output.emitWatermarkStatus(status);
            }

            @Override
            public void latency(long nodeId, LatencyMarker marker) {
                metricTree.latency(nodeId, marker);
                if (sharedOutputs == null && nodeId == controls.rootId()) {
                    reportOrForwardLatencyMarker(marker);
                }
            }
        };
        var restored =
                stateLifecycle == null ? java.util.Map.<Long, Long>of() : stateLifecycle.restoredWindowWatermarks();
        controls = sharedPlan == null
                ? new NativeRegionControlScheduler(
                        memory.executionContext().identifiedPlan(),
                        inputTypes.size(),
                        memory.executionContext().controlCapabilities(),
                        restored,
                        this::dispatchControl,
                        listener)
                : new NativeRegionControlScheduler(
                        sharedPlan,
                        memory.executionContext().controlCapabilities(),
                        restored,
                        this::dispatchControl,
                        listener);
        dispatcher = new ArrowNativeRegionDispatcher(
                memory.executionContext(),
                inputTypes,
                outputTypes,
                memory.allocator(),
                controls.processingTimeInputPorts(),
                getProcessingTimeService()::getCurrentProcessingTime);
        processingTimers = new NativeRegionProcessingTimeScheduler(
                controls, getProcessingTimeService(), memory.executionContext()::processingTimeDeadlines);
        processingTimers.refresh();
    }

    @Override
    public List<Input> getInputs() {
        return inputs;
    }

    // A one-port region can be chained after its source-edge adapter. Flink invokes the
    // same port through OneInputStreamOperator there; a task head uses getInputs().
    @SuppressWarnings("unchecked")
    private Input<Object> singleInput() {
        if (inputs.size() != 1)
            throw new IllegalStateException("A multi-port native region cannot be a chained one-input operator");
        return (Input<Object>) inputs.get(0);
    }

    @Override
    public void processElement(StreamRecord<Object> record) throws Exception {
        singleInput().processElement(record);
    }

    @Override
    public void setKeyContextElement(StreamRecord<Object> record) throws Exception {
        singleInput().setKeyContextElement(record);
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        singleInput().processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status) throws Exception {
        singleInput().processWatermarkStatus(status);
    }

    @Override
    public void processLatencyMarker(LatencyMarker marker) throws Exception {
        singleInput().processLatencyMarker(marker);
    }

    @Override
    public void processRecordAttributes(org.apache.flink.streaming.runtime.streamrecord.RecordAttributes attributes)
            throws Exception {
        singleInput().processRecordAttributes(attributes);
    }

    @Override
    public void processWatermark(org.apache.flink.runtime.event.WatermarkEvent watermark) throws Exception {
        singleInput().processWatermark(watermark);
    }

    @Override
    protected void reportWatermark(Watermark watermark, int inputId) throws Exception {
        controls.watermark(inputId - 1, watermark.getTimestamp());
        processingTimers.refresh();
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus status, int inputId) throws Exception {
        controls.status(inputId - 1, status);
        processingTimers.refresh();
    }

    // Flink's StreamOperatorWrapper gives BoundedOneInput precedence over BoundedMultiInput.
    // Use only the port-aware contract: Flink passes port 1 for a chained single-input region too.
    @Override
    public void endInput(int inputId) throws Exception {
        int port = java.util.Objects.checkIndex(inputId - 1, ended.length);
        if (!ended[port]) {
            controls.endInput(port);
            processingTimers.refresh();
            ended[port] = true;
        }
    }

    @Override
    public void finish() throws Exception {
        controls.finish();
        processingTimers.close();
        java.util.Arrays.fill(ended, true);
        super.finish();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        controls.beforeCheckpoint(checkpointId);
        processingTimers.refresh();
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

    private void emitOutput(int port, ArrowRowDataBatch batch) {
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
        if (port == 0) output.collect(new StreamRecord<>(batch));
        else output.collect(sharedOutputs.outputTag(port), new StreamRecord<>(batch));
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
        processingTimers.refresh();
    }

    @Override
    public void close() throws Exception {
        try {
            org.apache.flink.util.IOUtils.closeAll(
                    processingTimers, dispatcher, metricTree, stateLifecycle == null ? memory : stateLifecycle);
        } finally {
            processingTimers = null;
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
                processingTimers.refresh();
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
