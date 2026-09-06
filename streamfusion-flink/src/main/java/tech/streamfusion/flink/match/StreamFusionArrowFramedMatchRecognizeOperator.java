/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.match;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowMatchRecognizeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMatchRecognizeBridge;

/** Bounded MATCH_RECOGNIZE that decodes its framed network input in the consuming task. */
final class StreamFusionArrowFramedMatchRecognizeOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<NativeExchangeFrame, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType inputType;
    private final RowType outputType;
    private final byte[] exchangePlan;

    private transient Counter numLateRecordsDropped;
    private transient Counter completedMatches;
    private transient long[] observedStatistics;

    StreamFusionArrowFramedMatchRecognizeOperator(
            RowType inputType, RowType outputType, byte[] matchPlan, byte[] exchangePlan) {
        super(matchPlan, "bounded match recognize", NativeMatchRecognizeBridge.keyedStateBridge());
        this.inputType = inputType;
        this.outputType = outputType;
        this.exchangePlan = exchangePlan.clone();
    }

    @Override
    public void open() throws Exception {
        super.open();
        numLateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
        completedMatches = getMetricGroup().addGroup("StreamFusion").counter("matchRecognizeCompletedMatches");
        observedStatistics = NativeMatchRecognizeBridge.statistics(nativeHandle());
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) throws Exception {
        try (ArrowExchangeInputBatch decoded = ArrowExchangeInputCDataBridge.decode(
                        exchangePlan, element.getValue(), inputType, allocator(), memoryManager());
                ArrowRowDataBatch result = ArrowMatchRecognizeCDataBridge.processExchangeInput(
                        nativeHandle(), decoded, outputType, allocator(), memoryManager())) {
            int physicalOutput = 0;
            if (result.size() > 0) {
                output.collect(new StreamRecord<>(result));
                physicalOutput = 1;
            }
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, decoded.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
            recordProcessedWithoutStateCalls(decoded.size(), result);
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeMatchRecognizeBridge.statistics(nativeHandle());
        if (current.length != 3 || observedStatistics.length != 3) {
            throw new IllegalStateException("Native match recognize statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1], 0, 0, 0);
        completedMatches.inc(current[2] - observedStatistics[2]);
        observedStatistics = current;
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder().setInternalSorterSupported(true).build();
    }
}
