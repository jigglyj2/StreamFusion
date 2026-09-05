/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class BoundedSortMergeJoinParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void forcedSortMergeJoinMatchesFlinkByteForByte(String ignoredName, String query) throws Exception {
        byte[] flinkResult = execute(query, false);
        byte[] streamFusionResult = execute(query, true);

        assertThat(streamFusionResult).isEqualTo(flinkResult);
        assertThat(StreamFusionPlannerFactory.nativeRegularJoinBatchCount())
                .as(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(String sql, boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
        TableEnvironment tables = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tables.getConfig().getConfiguration().set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin");
        tables.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE);
        tables.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
                        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.NONE);
        return collect(tables.executeSql(sql));
    }

    private static Stream<Arguments> queries() {
        String left = "(VALUES (1, 'left-1', ARRAY[1, 2]), (1, 'left-2', ARRAY[3]), "
                + "(2, 'left-3', ARRAY[4, 5]), (CAST(NULL AS INT), 'left-null', ARRAY[6])) "
                + "AS l(id, payload, items)";
        String right = "(VALUES (1, 'right-1', MAP['a', 1]), (3, 'right-3', MAP['b', 2]), "
                + "(CAST(NULL AS INT), 'r-nullxxx', MAP['c', 3])) AS r(id, payload, attributes)";
        return Stream.of(
                Arguments.of(
                        "inner with duplicate and nested payloads",
                        "SELECT l.id, l.payload, l.items, r.payload, r.attributes FROM " + left + " JOIN " + right
                                + " ON l.id = r.id"),
                Arguments.of(
                        "left outer",
                        "SELECT l.id, l.payload, r.payload FROM " + left + " LEFT JOIN " + right + " ON l.id = r.id"),
                Arguments.of(
                        "right outer",
                        "SELECT l.id, l.payload, r.id, r.payload FROM " + left + " RIGHT JOIN " + right
                                + " ON l.id = r.id"),
                Arguments.of(
                        "full outer",
                        "SELECT l.id, l.payload, r.id, r.payload FROM " + left + " FULL OUTER JOIN " + right
                                + " ON l.id = r.id"),
                Arguments.of(
                        "semi",
                        "SELECT l.id, l.payload FROM " + left + " WHERE l.id IN (SELECT r.id FROM " + right + ")"),
                Arguments.of(
                        "residual predicate",
                        "SELECT l.id, l.payload, r.payload FROM " + left + " JOIN " + right
                                + " ON l.id = r.id AND l.payload <> r.payload"));
    }
}
