/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Verifies ordinary admission and execution of the official legacy CSV lookup query. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ13PlanningIT {
    @TempDir
    Path directory;

    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @CsvSource({"false,hashmap", "true,hashmap", "false,rocksdb", "true,rocksdb"})
    void legacyFilesystemLookupRunsOnFlinkAndInTheNativeRegion(boolean selected, String backend) throws Exception {
        long bids = NexmarkRowDataJob.runBlackhole(10_000, "q0", false, backend, 1, false);
        assertThat(bids).isPositive();
        clearPlanner();
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.executeSql(NexmarkRowDataJob.sourceDdl(10_000));
        NexmarkSqlJob.createViews(tables);
        Path input = directory.resolve("side_input.txt");
        // Match upstream SideInputGenerator's exact integer/string rows without executing its CLI.
        Files.write(input, IntStream.range(0, 10_000).mapToObj(i -> i + "," + i).collect(Collectors.toList()));
        String sql;
        try (var stream = getClass().getResourceAsStream("/queries/q13.sql")) {
            assertThat(stream).isNotNull();
            sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
                    .replace(
                            "file://${FLINK_HOME}/data/side_input.txt",
                            input.toUri().toString())
                    .replace("nexmark_q13", "nexmark_output");
        }
        String[] statements = sql.split(";");
        assertThat(statements).hasSize(3);
        tables.executeSql(statements[0]);
        tables.executeSql(statements[1]);
        String plan = tables.explainSql(statements[2]);
        assertThat(plan).contains("LookupJoin", "side_input");
        if (selected) assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        try (var metrics = NexmarkBlackholeMetrics.begin()) {
            NexmarkBlackholeMetrics.configure(tables.getConfig().getConfiguration(), metrics.id);
            tables.executeSql(statements[2]).await();
            // The original side input covers every modulo key with exactly one match.
            assertThat(metrics.outputRows()).isEqualTo(bids);
        }
        if (selected)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        else assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}
