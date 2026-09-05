/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowMultiJoinCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.metrics.FlinkMetricParity;
import tech.streamfusion.flink.state.AbstractStreamFusionArrowKeyedStateOperatorV2;
import tech.streamfusion.nativebridge.NativeMultiJoinBridge;

/** Native N-input streaming join over ordered, backend-neutral Arrow-row multiset state. */
final class StreamFusionArrowMultiJoinOperator extends AbstractStreamFusionArrowKeyedStateOperatorV2
        implements MultipleInputStreamOperator<ArrowRowDataBatch>, BoundedMultiInput {
    private final List<RowType> inputTypes;
    private final RowType outputType;
    private final List<int[]> commonKeyFields;
    private final List<RowDataKeySelector> commonKeySelectors;
    private final List<Map<Integer, RowDataKeySelector>> conditionSelectors;
    private final List<byte[]> exchangePlans;

    private transient long[] inputWatermarks;
    private transient List<ListState<Long>> watermarkStates;
    private transient long[] observedStatistics;

    StreamFusionArrowMultiJoinOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters,
            List<RowType> inputTypes,
            RowType outputType,
            List<int[]> commonKeyFields,
            byte[] plan,
            List<RowDataKeySelector> commonKeySelectors,
            List<Map<Integer, RowDataKeySelector>> conditionSelectors,
            List<byte[]> exchangePlans) {
        super(parameters, inputTypes.size(), plan, "multi-join", NativeMultiJoinBridge.keyedStateBridge());
        this.inputTypes = List.copyOf(inputTypes);
        this.outputType = outputType;
        this.commonKeyFields = new ArrayList<>(commonKeyFields.size());
        for (int[] fields : commonKeyFields) {
            this.commonKeyFields.add(fields.clone());
        }
        this.commonKeySelectors = List.copyOf(commonKeySelectors);
        this.conditionSelectors = List.copyOf(conditionSelectors);
        this.exchangePlans = new ArrayList<>(exchangePlans.size());
        for (byte[] exchangePlan : exchangePlans) {
            this.exchangePlans.add(exchangePlan.clone());
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        observedStatistics = NativeMultiJoinBridge.statistics(nativeHandle());
        getMetricGroup().addGroup("StreamFusion").gauge("pendingEventTimeTimers", () -> 0L);
        getMetricGroup().addGroup("StreamFusion").gauge("pendingProcessingTimeTimers", () -> 0L);
    }

    @Override
    public List<Input> getInputs() {
        List<Input> inputs = new ArrayList<>(inputTypes.size());
        for (int input = 1; input <= inputTypes.size(); input++) {
            inputs.add(new MultiJoinInput(this, input));
        }
        return inputs;
    }

    @Override
    public void endInput(int inputId) {}

    @Override
    protected void afterNativeStateInitialized(StateInitializationContext context) throws Exception {
        inputWatermarks = new long[inputTypes.size()];
        java.util.Arrays.fill(inputWatermarks, Long.MIN_VALUE);
        watermarkStates = new ArrayList<>(inputTypes.size());
        for (int input = 0; input < inputTypes.size(); input++) {
            ListState<Long> state = context.getOperatorStateStore()
                    .getUnionListState(
                            new ListStateDescriptor<>("multi-join-watermark-" + input, LongSerializer.INSTANCE));
            watermarkStates.add(state);
            if (context.isRestored()) {
                for (Long candidate : state.get()) {
                    inputWatermarks[input] = Math.max(inputWatermarks[input], candidate);
                }
                combinedWatermark.updateWatermark(input, inputWatermarks[input]);
            }
        }
    }

    @Override
    protected void beforeNativeStateSnapshot(StateSnapshotContext context) throws Exception {
        for (int input = 0; input < inputTypes.size(); input++) {
            watermarkStates.get(input).update(Collections.singletonList(inputWatermarks[input]));
        }
    }

    private final class MultiJoinInput extends AbstractInput<Object, ArrowRowDataBatch> {
        private final int zeroBasedInput;

        private MultiJoinInput(AbstractStreamOperatorV2<ArrowRowDataBatch> owner, int inputId) {
            super(owner, inputId);
            zeroBasedInput = inputId - 1;
        }

        @Override
        public void processElement(StreamRecord<Object> element) throws Exception {
            if (!(element.getValue() instanceof NativeExchangeFrame)) {
                throw new IllegalStateException("StreamFusion multi-join requires frame-encoded keyed inputs");
            }
            try (ArrowExchangeInputBatch input = tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge.decode(
                    exchangePlans.get(zeroBasedInput),
                    (NativeExchangeFrame) element.getValue(),
                    inputTypes.get(zeroBasedInput),
                    allocator(),
                    memoryManager())) {
                processBatch(zeroBasedInput, input);
            }
        }

        @Override
        public void processWatermark(Watermark watermark) throws Exception {
            inputWatermarks[zeroBasedInput] = Math.max(inputWatermarks[zeroBasedInput], watermark.getTimestamp());
            long previous = combinedWatermark.getCombinedWatermark();
            reportWatermark(watermark, zeroBasedInput + 1);
            if (combinedWatermark.getCombinedWatermark() > previous) {
                recordWatermark();
            }
        }
    }

    private void processBatch(int inputIndex, ArrowExchangeInputBatch input) throws Exception {
        try {
            ArrowRowDataBatch visible = input.arrowBatch();
            List<byte[]> keys = requiresPreencodedKeys(inputTypes.get(inputIndex), commonKeyFields.get(inputIndex))
                    ? preencodeKeys(visible, commonKeySelectors.get(inputIndex), "multi-join")
                    : null;
            Map<Integer, List<byte[]>> conditions = new LinkedHashMap<>();
            for (Map.Entry<Integer, RowDataKeySelector> entry :
                    conditionSelectors.get(inputIndex).entrySet()) {
                List<byte[]> encoded = preencodeKeys(visible, entry.getValue(), "multi-join condition");
                FieldVectorNulls.apply(visible, entry.getKey(), encoded);
                conditions.put(entry.getKey(), encoded);
            }
            try (ArrowRowDataBatch result = ArrowMultiJoinCDataBridge.execute(
                    nativeHandle(), inputIndex, input, keys, conditions, outputType, allocator(), memoryManager())) {
                int physicalOutput = 0;
                if (result.size() > 0) {
                    output.collect(new StreamRecord<>(result));
                    physicalOutput = 1;
                }
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, input.size());
                FlinkMetricParity.replacePhysicalRecords(
                        getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), physicalOutput, result.size());
                recordProcessedWithoutStateCalls(input.size(), result);
            }
            updateNativeStatistics();
        } catch (Throwable failure) {
            recordProcessingFailure();
            throw failure;
        }
    }

    private void updateNativeStatistics() {
        long[] current = NativeMultiJoinBridge.statistics(nativeHandle());
        if (current.length != 2 || observedStatistics.length != 2) {
            throw new IllegalStateException("Native multi-join statistics have an incompatible shape");
        }
        recordNativeStateStatistics(current[0] - observedStatistics[0], current[1] - observedStatistics[1]);
        observedStatistics = current;
    }

    /** Keeps SQL null unequal while transporting opaque Flink key bytes for every other value. */
    private static final class FieldVectorNulls {
        private FieldVectorNulls() {}

        private static void apply(ArrowRowDataBatch batch, int field, List<byte[]> values) {
            for (int row = 0; row < batch.size(); row++) {
                if (batch.root().getVector(field).isNull(row)) {
                    values.set(row, null);
                }
            }
        }
    }
}
