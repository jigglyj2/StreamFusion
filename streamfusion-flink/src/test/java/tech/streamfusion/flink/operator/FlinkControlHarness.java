/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness;

final class FlinkControlHarness extends MultiInputStreamOperatorTestHarness<String> {
    FlinkControlHarness(int arity) throws Exception {
        super(new Factory(arity));
    }

    Input input(int index) {
        return getCastedOperator().getInputs().get(index);
    }

    private static final class Factory extends AbstractStreamOperatorFactory<String> {
        private final int arity;

        private Factory(int arity) {
            this.arity = arity;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
            return (T) new ReferenceOperator(parameters, arity);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader loader) {
            return ReferenceOperator.class;
        }
    }

    private static final class ReferenceOperator extends AbstractStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String> {
        private final List<Input> inputs = new ArrayList<>();

        private ReferenceOperator(StreamOperatorParameters<String> parameters, int arity) {
            super(parameters, arity);
            for (int index = 1; index <= arity; index++) {
                inputs.add(new AbstractInput<String, String>(this, index) {
                    @Override
                    public void processElement(StreamRecord<String> record) {
                        output.collect(record);
                    }
                });
            }
        }

        @Override
        public List<Input> getInputs() {
            return inputs;
        }
    }
}
