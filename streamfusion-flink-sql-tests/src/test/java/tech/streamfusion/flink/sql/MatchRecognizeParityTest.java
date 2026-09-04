/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class MatchRecognizeParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fixedProcessingTimeSequenceMatchesFlinkByteForByte(boolean skipPastLast) throws Exception {
        byte[] flink = execute(false, skipPastLast, "A B C", "");
        byte[] streamFusion = execute(true, skipPastLast, "A B C", "");

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeMatchRecognizeBatchCount()).isGreaterThan(0);
    }

    @Test
    void quantifiedPatternFallsBackWithAnExplicitReason() throws Exception {
        execute(true, false, "A+ B C", "");

        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: no")
                .contains("fixed strict concatenation");
        assertThat(StreamFusionPlannerFactory.nativeMatchRecognizeBatchCount()).isZero();
    }

    @Test
    void withinIntervalFallsBackUntilNativeTimersAreImplemented() throws Exception {
        execute(true, false, "A B C", " WITHIN INTERVAL '1' MINUTE");

        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: no")
                .contains("does not yet support WITHIN");
        assertThat(StreamFusionPlannerFactory.nativeMatchRecognizeBatchCount()).isZero();
    }

    private static byte[] execute(boolean streamFusion, boolean skipPastLast, String pattern, String patternSuffix)
            throws Exception {
        StreamFusionPlannerFactory.resetMetrics();
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Table input = tables.fromDataStream(
                environment.fromCollection(
                        List.of(
                                Row.of("x", 1L, "a"),
                                Row.of("y", 10L, "a"),
                                Row.of("x", 2L, "b"),
                                Row.of("x", 3L, "c"),
                                Row.of("y", 11L, "b"),
                                Row.of("x", 4L, "a"),
                                Row.of("y", 12L, "c"),
                                Row.of("x", 5L, "b"),
                                Row.of("x", 6L, "c")),
                        Types.ROW_NAMED(
                                new String[] {"category", "id", "label"}, Types.STRING, Types.LONG, Types.STRING)),
                Schema.newBuilder()
                        .column("category", DataTypes.STRING().notNull())
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("label", DataTypes.STRING().notNull())
                        .columnByExpression("pt", "PROCTIME()")
                        .build());
        tables.createTemporaryView("match_input", input);
        String after = skipPastLast ? "AFTER MATCH SKIP PAST LAST ROW" : "AFTER MATCH SKIP TO NEXT ROW";
        return collect(tables.executeSql("SELECT category, aid, bid, cid FROM match_input MATCH_RECOGNIZE ("
                + "PARTITION BY category ORDER BY pt "
                + "MEASURES A.id AS aid, B.id AS bid, C.id AS cid "
                + "ONE ROW PER MATCH "
                + after
                + " PATTERN ("
                + pattern
                + ")"
                + patternSuffix
                + " "
                + "DEFINE A AS label = 'a', B AS label = 'b', C AS label = 'c')"));
    }
}
