/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.util.List;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowAggregateCDataBridge;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeWindowAggregateBridge;

/** Bounded window aggregation that decodes its framed exchange input in the consuming task. */
final class StreamFusionArrowFramedWindowAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<NativeExchangeFrame, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType inputType;
    private final RowType outputType;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;
    private final byte[] exchangePlan;

    private transient long[] observedNativeStatistics;
    private transient boolean finished;

    StreamFusionArrowFramedWindowAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] aggregatePlan,
            RowDataKeySelector keySelector,
            byte[] exchangePlan) {
        super(aggregatePlan, "batch global window aggregate", NativeWindowAggregateBridge.keyedStateBridge());
        this.inputType = inputType;
        this.outputType = outputType;
        this.preencodeKeys = requiresPreencodedKeys(inputType, grouping);
        this.keySelector = keySelector;
        this.exchangePlan = exchangePlan.clone();
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedNativeStatistics = NativeWindowAggregateBridge.statistics(nativeHandle());
        MetricGroup diagnostics = getMetricGroup().addGroup("StreamFusion");
        diagnostics.gauge("pendingEventTimeTimers", () -> NativeWindowAggregateBridge.statistics(nativeHandle())[5]);
        diagnostics.gauge("pendingProcessingTimeTimers", () -> 0L);
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) throws Exception {
        try (ArrowExchangeInputBatch decoded = ArrowExchangeInputCDataBridge.decode(
                exchangePlan, element.getValue(), inputType, allocator(), memoryManager())) {
            ArrowRowDataBatch input = decoded.arrowBatch();
            for (int row = 0; row < input.size(); row++) {
                if (input.rowKind(row) != RowKind.INSERT) {
                    throw new IllegalStateException(
                            "Native bounded append-only window aggregate got " + input.rowKind(row));
                }
            }
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "batch window aggregate") : null;
            try (ArrowRowDataBatch result = ArrowWindowAggregateCDataBridge.process(
                    nativeHandle(), input, keys, false, 0L, outputType, allocator(), memoryManager())) {
                emit(result, true, input.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        // Flink's bounded hash/sort window executors emit only at end of input. Intermediate
        // watermarks are control records, not firing triggers for this physical operator.
        super.processWatermark(watermark);
    }

    @Override
    public void endInput() throws Exception {
        if (finished) {
            return;
        }
        finished = true;
        emitTimerOutput(Long.MAX_VALUE);
    }

    private void emitTimerOutput(long timestamp) throws Exception {
        try (ArrowRowDataBatch result = ArrowWindowAggregateCDataBridge.advance(
                nativeHandle(), false, timestamp, outputType, allocator(), memoryManager())) {
            emit(result, false, 0);
            recordTimerOutput(result, false);
        }
        updateNativeStatistics();
    }

    private void emit(ArrowRowDataBatch result, boolean hasInputBatch, int inputRows) {
        int physicalOutput = 0;
        if (result.size() > 0) {
            output.collect(new StreamRecord<>(result));
            physicalOutput = 1;
        }
        if (hasInputBatch) {
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, inputRows);
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
    }

    private void updateNativeStatistics() {
        long[] current = NativeWindowAggregateBridge.statistics(nativeHandle());
        if (current.length != 7 || observedNativeStatistics.length != 7) {
            throw new IllegalStateException("Native window statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedNativeStatistics[0],
                current[1] - observedNativeStatistics[1],
                current[2] - observedNativeStatistics[2],
                current[3] - observedNativeStatistics[3],
                current[4] - observedNativeStatistics[4]);
        observedNativeStatistics = current;
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnlyAfterEndOfStream(true)
                .setInternalSorterSupported(true)
                .build();
    }
}
