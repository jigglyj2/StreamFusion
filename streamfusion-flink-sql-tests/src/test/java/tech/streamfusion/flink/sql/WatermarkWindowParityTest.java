/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class WatermarkWindowParityTest extends SqlParityTestSupport {
    private static final String SQL = "SELECT ts, window_start, window_end "
            + "FROM TABLE(TUMBLE(TABLE watermark_input, DESCRIPTOR(ts), INTERVAL '5' SECOND))";

    @Test
    void watermarkAssignerAndNativeTumbleMatchFlinkEndToEnd() throws Exception {
        byte[] flink = execute(false);
        byte[] streamFusion = execute(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.createTemporaryView(
                "watermark_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of(LocalDateTime.of(2026, 8, 29, 12, 0, 1)),
                                        Row.of(LocalDateTime.of(2026, 8, 29, 12, 0, 7)),
                                        Row.of(LocalDateTime.of(2026, 8, 29, 12, 0, 4))),
                                Types.ROW_NAMED(new String[] {"ts"}, Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '2' SECOND")
                                .build()));
        return collect(tables.executeSql(SQL));
    }
}
