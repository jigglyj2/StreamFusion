/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.values;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowValuesCDataBridge;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;

/** Bounded native VALUES source with task-scoped Flink managed memory. */
@SuppressWarnings("deprecation")
final class StreamFusionValuesSourceOperator
        extends StreamSource<ArrowRowDataBatch, StreamFusionValuesSourceOperator.EmptySourceFunction> {
    private final byte[] serializedPlan;
    private final RowType outputType;
    private transient StreamFusionTaskMemory taskMemory;

    StreamFusionValuesSourceOperator(byte[] serializedPlan, RowType outputType) {
        super(new EmptySourceFunction(), false);
        this.serializedPlan = serializedPlan.clone();
        this.outputType = outputType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        taskMemory = StreamFusionTaskMemory.create(
                getContainingTask().getEnvironment(),
                getOperatorConfig(),
                getMetricGroup(),
                "streamfusion-values",
                serializedPlan);
    }

    @Override
    public void run(
            Object lockingObject,
            Output<StreamRecord<ArrowRowDataBatch>> collector,
            OperatorChain<?, ?> operatorChain) {
        try (ArrowRowDataBatch batch =
                ArrowValuesCDataBridge.execute(taskMemory.executionContext(), outputType, taskMemory.allocator())) {
            synchronized (lockingObject) {
                collector.collect(new StreamRecord<>(batch));
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (taskMemory != null) {
                taskMemory.close();
                taskMemory = null;
            }
        } finally {
            super.close();
        }
    }

    static final class EmptySourceFunction implements SourceFunction<ArrowRowDataBatch> {
        private static final long serialVersionUID = 1L;

        @Override
        public void run(SourceContext<ArrowRowDataBatch> context) {}

        @Override
        public void cancel() {}
    }
}
