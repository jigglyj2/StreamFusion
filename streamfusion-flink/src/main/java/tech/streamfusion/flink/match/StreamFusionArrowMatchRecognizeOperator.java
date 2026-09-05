/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.match;

import java.util.List;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowMatchRecognizeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeMatchRecognizeBridge;
import tech.streamfusion.proto.plan.v1.Expression;

/** Arrow-native fixed-sequence MATCH_RECOGNIZE. */
final class StreamFusionArrowMatchRecognizeOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType outputType;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;

    private transient Counter numLateRecordsDropped;
    private transient Counter completedMatches;
    private transient long[] observedStatistics;

    StreamFusionArrowMatchRecognizeOperator(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            List<String> variableNames,
            List<Expression> conditions,
            int[] measureVariables,
            int[] measureFields,
            boolean skipPastLastRow,
            RowDataKeySelector keySelector) {
        super(
                StreamFusionMatchRecognizePlan.create(
                        inputType,
                        outputType,
                        partitionKeys,
                        variableNames,
                        conditions,
                        measureVariables,
                        measureFields,
                        skipPastLastRow),
                "match recognize",
                NativeMatchRecognizeBridge.keyedStateBridge());
        this.outputType = outputType;
        this.preencodeKeys = requiresPreencodedKeys(inputType, partitionKeys);
        this.keySelector = keySelector;
    }

    @Override
    public void open() throws Exception {
        super.open();
        // Flink's CepOperator registers this counter for both event-time and processing-time
        // execution. This processing-time slice cannot receive a late row, so exact parity is zero.
        numLateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
        completedMatches = getMetricGroup().addGroup("StreamFusion").counter("matchRecognizeCompletedMatches");
        observedStatistics = NativeMatchRecognizeBridge.statistics(nativeHandle());
    }

    @Override
    public void processElement(StreamRecord<ArrowRowDataBatch> element) throws Exception {
        ArrowRowDataBatch input = element.getValue();
        try {
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "match recognize") : null;
            try (ArrowRowDataBatch result = ArrowMatchRecognizeCDataBridge.process(
                    nativeHandle(), input, keys, outputType, allocator(), memoryManager())) {
                int physicalOutput = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutput = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
                recordProcessedWithoutStateCalls(input, result);
            }
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
}
