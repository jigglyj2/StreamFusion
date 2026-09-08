/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

/** The real SQL-generated Flink local slicer, with its managed RecordsWindowBuffer. */
final class LocalWindowFlinkOracle {
    private LocalWindowFlinkOracle() {}

    @SuppressWarnings("unchecked")
    static OneInputStreamOperatorTestHarness<RowData, RowData> create(long memoryBytes) throws Exception {
        var stage = SlicingWindowFlinkPlan.stage("LocalWindowAggregate");
        return create(stage, memoryBytes);
    }

    @SuppressWarnings("unchecked")
    static OneInputStreamOperatorTestHarness<RowData, RowData> create(
            org.apache.flink.streaming.api.transformations.OneInputTransformation<?, ?> stage, long memoryBytes)
            throws Exception {
        var operator = (OneInputStreamOperator<RowData, RowData>) stage.getOperator();
        if (!operator.getClass().getSimpleName().equals("LocalSlicingWindowAggOperator"))
            throw new AssertionError("Expected original Flink local slicer, got " + operator.getClass());
        var outputType = ((InternalTypeInfo<RowData>) stage.getOutputType()).toRowType();
        var environment =
                new MockEnvironmentBuilder().setManagedMemorySize(memoryBytes).build();
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(operator, environment) {
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
            harness.getStreamConfig().setStateBackendUsesManagedMemory(false);
            harness.getStreamConfig().setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1.0);
            harness.setup(new RowDataSerializer(outputType));
            harness.open();
            return harness;
        } catch (Throwable failure) {
            harness.close();
            throw failure;
        }
    }
}
