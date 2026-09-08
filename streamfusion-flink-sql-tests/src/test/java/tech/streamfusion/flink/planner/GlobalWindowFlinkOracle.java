/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

/** Real Flink global HOP COUNT stage, accepting (key, partial count, slice end). */
final class GlobalWindowFlinkOracle {
    private GlobalWindowFlinkOracle() {}

    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            boolean rocks, OperatorSubtaskState restored) throws Exception {
        return create(rocks, restored, false);
    }

    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            boolean rocks, OperatorSubtaskState restored, boolean tumble) throws Exception {
        var stage = SlicingWindowFlinkPlan.stage("GlobalWindowAggregate", tumble);
        return create(stage, rocks, restored);
    }

    @SuppressWarnings("unchecked")
    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            org.apache.flink.streaming.api.transformations.OneInputTransformation<?, ?> stage,
            boolean rocks,
            OperatorSubtaskState restored)
            throws Exception {
        var operator = (OneInputStreamOperator<RowData, RowData>) stage.getOperator();
        if (!(operator instanceof WindowAggOperator<?, ?>))
            throw new AssertionError("Expected Flink WindowAggOperator");
        var outputType = ((InternalTypeInfo<RowData>) stage.getOutputType()).toRowType();
        var environment =
                new MockEnvironmentBuilder().setManagedMemorySize(64L << 20).build();
        var harness =
                new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                        operator,
                        (KeySelector<RowData, RowData>) stage.getStateKeySelector(),
                        (InternalTypeInfo<RowData>) stage.getStateKeyType(),
                        environment) {
                    @Override
                    public void close() throws Exception {
                        try {
                            super.close();
                        } finally {
                            environment.close();
                        }
                    }
                };
        try {
            harness.setStateBackend(
                    rocks
                            ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                            : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
            harness.getStreamConfig().setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            harness.getStreamConfig()
                    .setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 1.0);
            harness.setup(new RowDataSerializer(outputType));
            if (restored != null) harness.initializeState(restored);
            harness.open();
            return harness;
        } catch (Throwable failure) {
            harness.close();
            throw failure;
        }
    }
}
