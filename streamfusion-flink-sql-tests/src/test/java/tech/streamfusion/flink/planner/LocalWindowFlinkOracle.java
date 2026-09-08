/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** The real SQL-generated Flink local slicer, with its managed RecordsWindowBuffer. */
final class LocalWindowFlinkOracle {
    private LocalWindowFlinkOracle() {}

    @SuppressWarnings("unchecked")
    static OneInputStreamOperatorTestHarness<RowData, RowData> create(long memoryBytes) throws Exception {
        String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var tables = StreamTableEnvironment.create(env);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            tables.getConfig()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
            var source = env.fromCollection(
                    List.of(Row.of(1L, LocalDateTime.of(2026, 1, 1, 0, 0))),
                    Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME));
            tables.createTemporaryView(
                    "local_window_input",
                    tables.fromDataStream(
                            source,
                            Schema.newBuilder()
                                    .column("k", org.apache.flink.table.api.DataTypes.BIGINT())
                                    .column("ts", org.apache.flink.table.api.DataTypes.TIMESTAMP(3))
                                    .watermark("ts", "ts")
                                    .build()));
            var output = tables.toChangelogStream(tables.sqlQuery("SELECT k, COUNT(*) AS n, window_start, window_end "
                    + "FROM TABLE(HOP(TABLE local_window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) "
                    + "GROUP BY k, window_start, window_end"));
            var stage = find(output.getTransformation());
            if (stage == null) throw new AssertionError("Flink did not plan a local window stage");
            var operator = (OneInputStreamOperator<RowData, RowData>) stage.getOperator();
            if (!operator.getClass().getSimpleName().equals("LocalSlicingWindowAggOperator"))
                throw new AssertionError("Expected original Flink local slicer, got " + operator.getClass());
            var inputType = ((InternalTypeInfo<RowData>) stage.getInputType()).toRowType();
            var outputType = ((InternalTypeInfo<RowData>) stage.getOutputType()).toRowType();
            if (inputType.getFieldCount() != 2 || outputType.getFieldCount() != 3)
                throw new AssertionError("Unexpected Flink COUNT partial schemas: " + inputType + " / " + outputType);
            var environment = new MockEnvironmentBuilder()
                    .setManagedMemorySize(memoryBytes)
                    .build();
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
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    private static OneInputTransformation<?, ?> find(Transformation<?> node) {
        if (node instanceof OneInputTransformation<?, ?> && node.getName().contains("LocalWindowAggregate"))
            return (OneInputTransformation<?, ?>) node;
        for (var child : node.getInputs()) {
            var found = find(child);
            if (found != null) return found;
        }
        return null;
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
