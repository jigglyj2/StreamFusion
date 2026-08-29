/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.union;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.metrics.FlinkMetricParity;

/** Arrow-native UNION ALL; arrival order and control-input coordination remain with Flink. */
final class StreamFusionArrowUnionOperator extends AbstractStreamOperatorV2<ArrowRowDataBatch>
        implements MultipleInputStreamOperator<ArrowRowDataBatch>, BoundedMultiInput {
    private final int inputCount;

    StreamFusionArrowUnionOperator(StreamOperatorParameters<ArrowRowDataBatch> parameters, int inputCount) {
        super(parameters, inputCount);
        if (inputCount < 2) {
            throw new IllegalArgumentException("StreamFusion UNION ALL requires at least two inputs");
        }
        this.inputCount = inputCount;
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

    private final class UnionInput extends AbstractInput<ArrowRowDataBatch, ArrowRowDataBatch> {
        private UnionInput(AbstractStreamOperatorV2<ArrowRowDataBatch> owner, int inputId) {
            super(owner, inputId);
        }

        @Override
        public void processElement(StreamRecord<ArrowRowDataBatch> element) {
            ArrowRowDataBatch batch = element.getValue();
            output.collect(element);
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter(), 1, batch.size());
            FlinkMetricParity.replacePhysicalRecords(
                    getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter(), 1, batch.size());
        }
    }
}
