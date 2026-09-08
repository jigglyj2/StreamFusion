/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Shared SQL-generated local/global window fixtures; no StreamFusion rewrite participates. */
final class SlicingWindowFlinkPlan {
    private SlicingWindowFlinkPlan() {}

    static OneInputTransformation<?, ?> stage(String name) throws Exception {
        return stage(name, false);
    }

    static OneInputTransformation<?, ?> stage(String name, boolean tumble) throws Exception {
        return stage(name, tumble, false);
    }

    static OneInputTransformation<?, ?> stage(String name, boolean tumble, boolean stringKey) throws Exception {
        return stage(
                name,
                "SELECT k, COUNT(*) AS n, window_start, window_end FROM TABLE("
                        + (tumble ? "TUMBLE" : "HOP")
                        + "(TABLE local_window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND"
                        + (tumble ? "" : ", INTERVAL '6' SECOND")
                        + ")) GROUP BY k, window_start, window_end",
                stringKey);
    }

    static OneInputTransformation<?, ?> stage(String name, String sql) throws Exception {
        return stage(name, sql, false);
    }

    private static OneInputTransformation<?, ?> stage(String name, String sql, boolean stringKey) throws Exception {
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
                    List.of(Row.of(stringKey ? "key" : 1L, LocalDateTime.of(2026, 1, 1, 0, 0))),
                    Types.ROW_NAMED(
                            new String[] {"k", "ts"}, stringKey ? Types.STRING : Types.LONG, Types.LOCAL_DATE_TIME));
            tables.createTemporaryView(
                    "local_window_input",
                    tables.fromDataStream(
                            source,
                            Schema.newBuilder()
                                    .column(
                                            "k",
                                            stringKey
                                                    ? org.apache.flink.table.api.DataTypes.STRING()
                                                    : org.apache.flink.table.api.DataTypes.BIGINT())
                                    .column("ts", org.apache.flink.table.api.DataTypes.TIMESTAMP(3))
                                    .watermark("ts", "ts")
                                    .build()));
            var output = tables.toChangelogStream(tables.sqlQuery(sql));
            var stage = find(output.getTransformation(), name);
            if (stage == null) throw new AssertionError("Flink did not plan " + name);
            return stage;
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    private static OneInputTransformation<?, ?> find(Transformation<?> node, String name) {
        if (node instanceof OneInputTransformation<?, ?> && node.getName().contains(name))
            return (OneInputTransformation<?, ?>) node;
        for (var child : node.getInputs()) {
            var found = find(child, name);
            if (found != null) return found;
        }
        return null;
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
