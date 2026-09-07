/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.SelectedAggregateSqlProbe;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Native shared execution is mandatory here; the ordinary fallback-only type suite is separate. */
class SelectedAggregateTypeSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void allSupportedFlinkKeyFamiliesAndCountPayloadsExecuteWithGeneratedRetractions() throws Exception {
        List<Object[]> types = SelectDistinctFallbackParityTest.distinctTypes()
                .map(arguments -> arguments.get())
                .collect(java.util.stream.Collectors.toList());
        String keys = java.util.stream.IntStream.range(0, types.size())
                .mapToObj(index -> "c" + index)
                .collect(java.util.stream.Collectors.joining(", "));
        String counts = java.util.stream.IntStream.range(0, types.size())
                .mapToObj(index -> "COUNT(c" + index + ")")
                .collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT " + keys + ", COUNT(*), SUM(amount), " + counts + " FROM typed_input GROUP BY " + keys;
        for (boolean rocks : List.of(false, true))
            for (long bundle : List.of(0L, 7L)) {
                byte[] expected = execute(sql, types, rocks, bundle, false);
                String graph = SelectedAggregateSqlProbe.inputGraph;
                byte[] actual = execute(sql, types, rocks, bundle, true);
                assertThat(SelectedAggregateSqlProbe.inputGraph).isEqualTo(graph);
                assertThat(actual).as("rocks=" + rocks + " bundle=" + bundle).isEqualTo(expected);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isGreaterThan(0);
                assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                        .isZero();
                SelectedAggregateSqlProbe.verifyTranslatedArrowTopology();
            }
    }

    private static byte[] execute(String sql, List<Object[]> types, boolean rocks, long bundle, boolean selected)
            throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        SelectedAggregateSqlProbe.convertedGraphs = 0;
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, bundle > 0);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, Math.max(1, bundle));
        tables.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, java.time.Duration.ofHours(1));
        tables.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.ONE_PHASE);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().setIdleStateRetention(java.time.Duration.ZERO);
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
        if (selected) StreamFusionStateBackendFactory.install(tables.getConfig().getConfiguration());
        var names = new String[types.size() + 1];
        var information = new TypeInformation<?>[names.length];
        var schema = Schema.newBuilder();
        for (int index = 0; index < types.size(); index++) {
            names[index] = "c" + index;
            information[index] = (TypeInformation<?>) types.get(index)[1];
            schema.column(names[index], (DataType) types.get(index)[2]);
        }
        names[types.size()] = "amount";
        information[types.size()] = Types.LONG;
        schema.column("amount", org.apache.flink.table.api.DataTypes.BIGINT());
        tables.createTemporaryView(
                "typed_input",
                tables.fromChangelogStream(
                        environment.fromCollection(changes(types), Types.ROW_NAMED(names, information)),
                        schema.build(),
                        ChangelogMode.all()));
        SelectedAggregateSqlProbe.recordOnly = !selected;
        System.setProperty(
                StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, SelectedAggregateSqlProbe.class.getName());
        try {
            return collect(tables.executeSql(sql));
        } finally {
            System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        }
    }

    private static List<Row> changes(List<Object[]> types) {
        var keys = new ArrayList<Object[]>();
        Object[] first = types.stream().map(type -> type[3]).toArray();
        keys.add(first);
        // Change one field at a time: a dropped/mis-hashed field cannot hide behind other keys.
        for (int field = 0; field < types.size(); field++) {
            var second = first.clone();
            second[field] = types.get(field)[4];
            keys.add(second);
            var nullable = first.clone();
            nullable[field] = null;
            keys.add(nullable);
        }
        keys.add(new Object[types.size()]);
        var result = new ArrayList<Row>();
        for (int seed = 0; seed < 3; seed++) {
            var ordered = new ArrayList<>(keys);
            Collections.shuffle(ordered, new Random(seed));
            for (int index = 0; index < ordered.size(); index++) {
                Object[] key = ordered.get(index);
                result.add(change(key, index + 1L, RowKind.INSERT));
                result.add(change(key, -(index + 2L), RowKind.UPDATE_AFTER));
            }
            for (int index = ordered.size() - 1; index >= 0; index--) {
                Object[] key = ordered.get(index);
                result.add(change(key, index + 1L, RowKind.UPDATE_BEFORE));
                result.add(change(key, -(index + 2L), RowKind.DELETE));
            }
        }
        return result;
    }

    private static Row change(Object[] key, long amount, RowKind kind) {
        Object[] fields = java.util.Arrays.copyOf(key, key.length + 1);
        fields[key.length] = amount;
        return Row.ofKind(kind, fields);
    }
}
