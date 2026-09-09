/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Extracts the actual SQL-planned Flink operator, including its generated aggregate handler. */
final class DistinctCountFlinkOracle {
    static final RowType INPUT = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new VarCharType(), new BigIntType(), new org.apache.flink.table.types.logical.BooleanType()
            },
            new String[] {"k", "v", "selected"});
    static final RowType OUTPUT = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new VarCharType(), new BigIntType(false), new BigIntType(false), new BigIntType(false)
            },
            new String[] {"k", "n", "filtered", "all_rows"});

    private DistinctCountFlinkOracle() {}

    @SuppressWarnings("unchecked")
    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(boolean rocks) throws Exception {
        return create(rocks, null);
    }

    @SuppressWarnings("unchecked")
    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            boolean rocks, org.apache.flink.runtime.checkpoint.OperatorSubtaskState restore) throws Exception {
        return create(rocks, restore, true);
    }

    @SuppressWarnings("unchecked")
    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            boolean rocks, org.apache.flink.runtime.checkpoint.OperatorSubtaskState restore, boolean incremental)
            throws Exception {
        return create(rocks, restore, incremental, false);
    }

    @SuppressWarnings("unchecked")
    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            boolean rocks,
            org.apache.flink.runtime.checkpoint.OperatorSubtaskState restore,
            boolean incremental,
            boolean appendOnly)
            throws Exception {
        String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var tables = StreamTableEnvironment.create(env);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
            tables.getConfig().setIdleStateRetention(java.time.Duration.ZERO);
            var source = env.fromCollection(
                    List.of(Row.of("unused", 0L, true)),
                    Types.ROW_NAMED(new String[] {"k", "v", "selected"}, Types.STRING, Types.LONG, Types.BOOLEAN));
            tables.createTemporaryView(
                    "aggregate_input",
                    tables.fromChangelogStream(
                            source,
                            Schema.newBuilder().build(),
                            appendOnly ? ChangelogMode.insertOnly() : ChangelogMode.all()));
            var output = tables.toChangelogStream(
                    tables.sqlQuery(
                            "SELECT k, COUNT(DISTINCT v) AS n, COUNT(DISTINCT v) FILTER (WHERE selected) AS filtered, COUNT(*) AS all_rows FROM aggregate_input GROUP BY k"));
            var stage = find(output.getTransformation());
            var operator = (OneInputStreamOperator<RowData, RowData>) stage.getOperator();
            if (!operator.getClass()
                    .getName()
                    .equals("org.apache.flink.streaming.api.operators.KeyedProcessOperator")) {
                throw new AssertionError("Expected original Flink aggregate, got " + operator.getClass());
            }
            var harness = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                    operator,
                    (org.apache.flink.api.java.functions.KeySelector<RowData, RowData>) stage.getStateKeySelector(),
                    (InternalTypeInfo<RowData>) stage.getStateKeyType(),
                    16,
                    1,
                    0);
            harness.setStateBackend(
                    rocks
                            ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(incremental)
                            : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
            harness.setup(new org.apache.flink.table.runtime.typeutils.RowDataSerializer(OUTPUT));
            if (restore != null) harness.initializeState(restore);
            harness.open();
            return harness;
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    private static OneInputTransformation<?, ?> find(Transformation<?> node) {
        if (node instanceof OneInputTransformation<?, ?> && node.getName().contains("GroupAggregate"))
            return (OneInputTransformation<?, ?>) node;
        for (var child : node.getInputs()) {
            var result = find(child);
            if (result != null) return result;
        }
        return null;
    }

    private static void restore(String name, String value) {
        if (value == null) System.clearProperty(name);
        else System.setProperty(name, value);
    }
}
