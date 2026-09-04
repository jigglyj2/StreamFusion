/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

class CountAggregateTypeParityTest extends SqlParityTestSupport {
    @Test
    void countAcceptsEveryArrowBackedFlinkValueTypeWithoutDecodingPayloads() throws Exception {
        byte[] flink = execute(false);
        byte[] streamFusion = execute(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(boolean streamFusion) throws Exception {
        if (streamFusion) {
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

        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("x", 1);
        Map<String, Integer> multiset = new LinkedHashMap<>();
        multiset.put("x", 2);
        tables.createTemporaryView(
                "count_type_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of(
                                                "a",
                                                new byte[] {1, 2},
                                                new Integer[] {1, null},
                                                map,
                                                multiset,
                                                Row.of("nested", 7)),
                                        Row.of("a", null, null, null, null, null)),
                                Types.ROW_NAMED(
                                        new String[] {
                                            "category",
                                            "binary_value",
                                            "array_value",
                                            "map_value",
                                            "multiset_value",
                                            "row_value"
                                        },
                                        Types.STRING,
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.OBJECT_ARRAY(Types.INT),
                                        Types.MAP(Types.STRING, Types.INT),
                                        Types.MAP(Types.STRING, Types.INT),
                                        Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT))),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("binary_value", DataTypes.BYTES())
                                .column("array_value", DataTypes.ARRAY(DataTypes.INT()))
                                .column(
                                        "map_value",
                                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()))
                                .column(
                                        "multiset_value",
                                        DataTypes.MULTISET(DataTypes.STRING().notNull()))
                                .column(
                                        "row_value",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("amount", DataTypes.INT())))
                                .build()));
        return collect(tables.executeSql("SELECT category, COUNT(binary_value), COUNT(array_value), "
                + "COUNT(map_value), COUNT(multiset_value), COUNT(row_value) "
                + "FROM count_type_input GROUP BY category"));
    }
}
