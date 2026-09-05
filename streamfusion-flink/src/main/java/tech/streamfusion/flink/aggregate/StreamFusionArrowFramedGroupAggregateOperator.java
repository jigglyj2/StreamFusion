/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import java.util.List;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperator;
import tech.streamfusion.nativebridge.NativeGroupAggregateBridge;

/** Bounded keyed aggregate that decodes its framed network input in the consuming task. */
final class StreamFusionArrowFramedGroupAggregateOperator extends AbstractStreamFusionArrowKeyedStateOperator
        implements OneInputStreamOperator<NativeExchangeFrame, ArrowRowDataBatch>, BoundedOneInput {
    private final RowType inputType;
    private final RowType outputType;
    private final boolean preencodeKeys;
    private final RowDataKeySelector keySelector;
    private final byte[] exchangePlan;
    private final boolean hashAggregateMetrics;

    private transient long[] observedNativeStatistics;
    private transient boolean finished;

    StreamFusionArrowFramedGroupAggregateOperator(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            byte[] aggregatePlan,
            RowDataKeySelector keySelector,
            byte[] exchangePlan,
            String operatorName,
            boolean hashAggregateMetrics) {
        super(aggregatePlan, operatorName, NativeGroupAggregateBridge.keyedStateBridge());
        this.inputType = inputType;
        this.outputType = outputType;
        this.preencodeKeys = requiresPreencodedKeys(inputType, grouping);
        this.keySelector = keySelector;
        this.exchangePlan = exchangePlan.clone();
        this.hashAggregateMetrics = hashAggregateMetrics;
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedNativeStatistics = NativeGroupAggregateBridge.statistics(nativeHandle());
        if (hashAggregateMetrics) {
            getMetricGroup().gauge("memoryUsedSizeInBytes", this::managedMemoryUsed);
            getMetricGroup().gauge("numSpillFiles", () -> 0L);
            getMetricGroup().gauge("spillInBytes", () -> 0L);
        }
    }

    @Override
    public void processElement(StreamRecord<NativeExchangeFrame> element) throws Exception {
        try (ArrowExchangeInputBatch decoded = ArrowExchangeInputCDataBridge.decode(
                exchangePlan, element.getValue(), inputType, allocator(), memoryManager())) {
            ArrowRowDataBatch input = decoded.arrowBatch();
            for (int row = 0; row < input.size(); row++) {
                if (input.rowKind(row) != RowKind.INSERT) {
                    throw new IllegalStateException(
                            "Native bounded append-only group aggregate got " + input.rowKind(row));
                }
            }
            List<byte[]> keys = preencodeKeys ? preencodeKeys(input, keySelector, "batch group aggregate") : null;
            try (ArrowRowDataBatch result = ArrowGroupAggregateCDataBridge.execute(
                    nativeHandle(), input, keys, false, outputType, allocator(), memoryManager())) {
                emit(result);
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                recordProcessedWithoutStateCalls(input, result);
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    @Override
    public void endInput() throws Exception {
        if (finished) {
            return;
        }
        finished = true;
        try {
            while (true) {
                try (ArrowRowDataBatch result = ArrowGroupAggregateCDataBridge.finishBundle(
                        nativeHandle(), outputType, allocator(), memoryManager())) {
                    if (result.size() == 0) {
                        break;
                    }
                    emit(result);
                    recordProcessedWithoutStateCalls(0, result);
                }
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void emit(ArrowRowDataBatch result) {
        int physical = 0;
        if (result.size() > 0) {
            output.collect(new StreamRecord<>(result));
            physical = 1;
        }
        FlinkMetricParity.replacePhysicalRecords(
                getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physical, result.size());
    }

    private void updateNativeStatistics() {
        long[] current = NativeGroupAggregateBridge.statistics(nativeHandle());
        if (current.length != 2 || observedNativeStatistics.length != 2) {
            throw new IllegalStateException("Native group aggregate statistics have an incompatible shape");
        }
        recordNativeWindowStatistics(
                current[0] - observedNativeStatistics[0], current[1] - observedNativeStatistics[1], 0, 0, 0);
        observedNativeStatistics = current;
    }

    @Override
    public OperatorAttributes getOperatorAttributes() {
        return new OperatorAttributesBuilder()
                .setOutputOnlyAfterEndOfStream(true)
                // Native bounded aggregation owns grouping and accumulation. Letting Flink wrap
                // the keyed input in SortingDataInput would sort every row unnecessarily and
                // reserve the task's managed memory before native state can use its assigned share.
                .setInternalSorterSupported(true)
                .build();
    }
}
