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
final class StringAggregateFlinkOracle {
    static final RowType INPUT = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new VarCharType(),
                new VarCharType(),
                new VarCharType(),
                new org.apache.flink.table.types.logical.BooleanType(),
                new BigIntType()
            },
            new String[] {"k", "part", "v", "selected", "member_id"});
    static final RowType OUTPUT = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(),
                new VarCharType(), new BigIntType(false), new BigIntType(false), new BigIntType(false)
            },
            new String[] {"k", "part", "lo", "hi", "filtered", "members", "filtered_members", "all_rows"});

    private StringAggregateFlinkOracle() {}

    // StreamExecExchange.HASH uses a non-chainable KeyGroupStreamPartitioner. Its RowData
    // serializer supplies binary strings to the original aggregate, on both state backends.
    // Preserve that physical boundary here: Java-backed StringData has a different comparator.
    static RowData binaryInput(RowData row) throws java.io.IOException {
        var serializer = new org.apache.flink.table.runtime.typeutils.RowDataSerializer(INPUT);
        var bytes = new org.apache.flink.core.memory.DataOutputSerializer(128);
        serializer.serialize(row, bytes);
        return serializer.deserialize(new org.apache.flink.core.memory.DataInputDeserializer(bytes.getCopyOfBuffer()));
    }

    @SuppressWarnings("unchecked")
    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> create(
            boolean rocks, org.apache.flink.runtime.checkpoint.OperatorSubtaskState restore, boolean incremental)
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
                    List.of(Row.of("unused", "part", "value", true, 0L)),
                    Types.ROW_NAMED(
                            new String[] {"k", "part", "v", "selected", "member_id"},
                            Types.STRING,
                            Types.STRING,
                            Types.STRING,
                            Types.BOOLEAN,
                            Types.LONG));
            tables.createTemporaryView(
                    "aggregate_input",
                    tables.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.insertOnly()));
            var output = tables.toChangelogStream(
                    tables.sqlQuery(
                            "SELECT k, part, MIN(v) AS lo, MAX(v) AS hi, MAX(v) FILTER (WHERE selected) AS filtered, COUNT(DISTINCT member_id) AS members, COUNT(DISTINCT member_id) FILTER (WHERE selected) AS filtered_members, COUNT(*) AS all_rows FROM aggregate_input GROUP BY k, part"));
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
