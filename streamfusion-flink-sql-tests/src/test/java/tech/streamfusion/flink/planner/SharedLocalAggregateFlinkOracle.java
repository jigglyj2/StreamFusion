/* Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0 */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.*;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Original SQL-generated local bundle operator, not a hand-written accumulator oracle. */
final class SharedLocalAggregateFlinkOracle {
    @SuppressWarnings("unchecked")
    static OneInputStreamOperatorTestHarness<RowData, RowData> create(int size) throws Exception {
        String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var tables = StreamTableEnvironment.create(env);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, (long) size);
            tables.getConfig()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, java.time.Duration.ofHours(1));
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
            tables.getConfig()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
            var source = env.fromCollection(
                    List.of(Row.of("unused", 0L)), Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG));
            tables.createTemporaryView(
                    "local_input",
                    tables.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
            var result = tables.toChangelogStream(tables.sqlQuery(
                    "SELECT k, COUNT(*) AS n, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM local_input GROUP BY k"));
            var stage = result.getTransformation().getTransitivePredecessors().stream()
                    .filter(node -> node.getName().contains("LocalGroupAggregate"))
                    .filter(node -> node instanceof OneInputTransformation<?, ?>)
                    .map(node -> (OneInputTransformation<?, ?>) node)
                    .findFirst()
                    .orElseThrow();
            if (!stage.getOperator().getClass().getSimpleName().equals("MapBundleOperator"))
                throw new AssertionError("Expected original Flink local bundle: "
                        + stage.getOperator().getClass());
            var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(
                    (OneInputStreamOperator<RowData, RowData>) stage.getOperator());
            var outputType = ((InternalTypeInfo<RowData>) stage.getOutputType()).toRowType();
            harness.setup(new RowDataSerializer(outputType));
            harness.open();
            return harness;
        } finally {
            if (factory == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            if (processor == null) System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }
}
