/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class BoundedSortParityTest extends SqlParityTestSupport {
    private static final String NON_TEMPORAL_SORT = "__table.exec.sort.non-temporal.enabled__";

    @Test
    void fullSortPreservesFlinkOrderingNullsAndDuplicates() throws Exception {
        String sql = "SELECT id, label FROM (VALUES "
                + "(3, 'c'), (1, 'z'), (1, 'a'), (CAST(NULL AS INT), 'null'), (2, 'b'), (1, 'a')) "
                + "AS input(id, label) ORDER BY id ASC NULLS LAST, label DESC NULLS FIRST";

        List<String> flink = executeValues(sql, false);
        List<String> streamFusion = executeValues(sql, true);

        assertThat(streamFusion).containsExactlyElementsOf(flink);
        assertThat(streamFusion)
                .containsExactly("+I[1, z]", "+I[1, a]", "+I[1, a]", "+I[2, b]", "+I[3, c]", "+I[null, null]");
        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("tech.streamfusion.flink.sql.TopNOrderingTypeParityTest#orderableTypes")
    void everyFlinkOrderableTypeHasBoundedSortParity(
            String description, TypeInformation<?> type, DataType dataType, Object low, Object high) throws Exception {
        String sql = "SELECT metric FROM bounded_sort_order_input ORDER BY metric ASC NULLS LAST";
        List<Row> rows = List.of(Row.of(high), Row.of(low), Row.of(high), Row.of((Object) null));

        List<String> flink = executeOrderType(sql, type, dataType, rows, false);
        List<String> streamFusion = executeOrderType(sql, type, dataType, rows, true);

        assertThat(streamFusion).containsExactlyElementsOf(flink);
        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void floatingPointNanAndSignedZeroHaveBoundedSortParity() throws Exception {
        List<Row> floats = List.of(
                Row.of(Float.NaN),
                Row.of(-0.0F),
                Row.of(+0.0F),
                Row.of(Float.NEGATIVE_INFINITY),
                Row.of(Float.POSITIVE_INFINITY));
        List<String> flink = executeOrderType(
                "SELECT metric FROM bounded_sort_order_input ORDER BY metric ASC NULLS LAST",
                Types.FLOAT,
                DataTypes.FLOAT(),
                floats,
                false);
        List<String> streamFusion = executeOrderType(
                "SELECT metric FROM bounded_sort_order_input ORDER BY metric ASC NULLS LAST",
                Types.FLOAT,
                DataTypes.FLOAT(),
                floats,
                true);
        // Flink's generated comparator intentionally compares NaN with every value and signed
        // zeros with each other as equal. Its runtime sorter is unstable inside those equivalence
        // classes, so parity means an identical multiset and identical strict-order classes.
        assertThat(streamFusion).containsExactlyInAnyOrderElementsOf(flink);
        assertThat(strictFloatingOrder(streamFusion)).containsExactlyElementsOf(strictFloatingOrder(flink));

        List<Row> doubles = List.of(
                Row.of(Double.NaN),
                Row.of(-0.0D),
                Row.of(+0.0D),
                Row.of(Double.NEGATIVE_INFINITY),
                Row.of(Double.POSITIVE_INFINITY));
        flink = executeOrderType(
                "SELECT metric FROM bounded_sort_order_input ORDER BY metric DESC NULLS FIRST",
                Types.DOUBLE,
                DataTypes.DOUBLE(),
                doubles,
                false);
        streamFusion = executeOrderType(
                "SELECT metric FROM bounded_sort_order_input ORDER BY metric DESC NULLS FIRST",
                Types.DOUBLE,
                DataTypes.DOUBLE(),
                doubles,
                true);
        assertThat(streamFusion).containsExactlyInAnyOrderElementsOf(flink);
        assertThat(strictFloatingOrder(streamFusion)).containsExactlyElementsOf(strictFloatingOrder(flink));
    }

    @Test
    void updateBeforeUpdateAfterAndDeleteProduceFlinksFinalCountedMultiset() throws Exception {
        List<String> flink = executeRetractions(false);
        List<String> streamFusion = executeRetractions(true);

        assertThat(streamFusion).containsExactlyElementsOf(flink);
        assertThat(streamFusion).containsExactly("+I[1, one]", "+I[2, two-new]", "+I[3, three]");
        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
    }

    @Test
    void fullSortStatePreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = executeAllTypePayload(false);
        byte[] streamFusion = executeAllTypePayload(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeBoundedSortBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static List<String> executeValues(String sql, boolean streamFusion) throws Exception {
        StreamTableEnvironment tables = environment(streamFusion);
        return collectOrdered(tables.executeSql(sql));
    }

    private static List<String> executeOrderType(
            String sql, TypeInformation<?> type, DataType dataType, List<Row> rows, boolean streamFusion)
            throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        configure(streamFusion);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        configure(tables);
        DataStream<Row> input = environment.fromCollection(rows, Types.ROW_NAMED(new String[] {"metric"}, type));
        tables.createTemporaryView(
                "bounded_sort_order_input",
                tables.fromDataStream(
                        input, Schema.newBuilder().column("metric", dataType).build()));
        return collectOrdered(tables.executeSql(sql));
    }

    private static List<String> executeRetractions(boolean streamFusion) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        configure(streamFusion);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        configure(tables);
        Table changes = tables.fromChangelogStream(
                environment.fromCollection(
                        List.of(
                                changed(RowKind.INSERT, 3, "three"),
                                changed(RowKind.INSERT, 2, "two"),
                                changed(RowKind.INSERT, 1, "one"),
                                changed(RowKind.INSERT, 4, "four"),
                                changed(RowKind.DELETE, 4, "four"),
                                changed(RowKind.UPDATE_BEFORE, 2, "two"),
                                changed(RowKind.UPDATE_AFTER, 2, "two-new")),
                        Types.ROW_NAMED(new String[] {"id", "label"}, Types.INT, Types.STRING)),
                Schema.newBuilder()
                        .column("id", "INT")
                        .column("label", "STRING")
                        .build());
        tables.createTemporaryView("bounded_sort_changes", changes);
        return collectOrdered(
                tables.executeSql("SELECT id, label FROM bounded_sort_changes ORDER BY id ASC, label ASC"));
    }

    private static byte[] executeAllTypePayload(boolean streamFusion) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        configure(streamFusion);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        configure(tables);
        tables.createTemporaryView(
                "bounded_sort_all_types",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                List.of(
                                        payloadChange(RowKind.INSERT, 3L, "three", 3),
                                        payloadChange(RowKind.INSERT, 1L, "one", 1),
                                        payloadChange(RowKind.INSERT, 2L, "two", 2),
                                        payloadChange(RowKind.DELETE, 2L, "two", 2)),
                                Types.ROW_NAMED(
                                        new String[] {"order_value", "payload"},
                                        Types.LONG,
                                        TopNOpaquePayloadTypeParityTest.payloadTypeInformation())),
                        Schema.newBuilder()
                                .column("order_value", DataTypes.BIGINT().notNull())
                                .column("payload", TopNOpaquePayloadTypeParityTest.payloadDataType())
                                .build()));
        return collect(
                tables.executeSql("SELECT order_value, payload FROM bounded_sort_all_types ORDER BY order_value DESC"));
    }

    private static StreamTableEnvironment environment(boolean streamFusion) {
        configure(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        configure(tables);
        return tables;
    }

    private static void configure(StreamTableEnvironment tables) {
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().getConfiguration().setString(NON_TEMPORAL_SORT, "true");
    }

    private static void configure(boolean streamFusion) {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }

    private static Row changed(RowKind kind, int id, String label) {
        Row row = Row.of(id, label);
        row.setKind(kind);
        return row;
    }

    private static Row payloadChange(RowKind kind, long orderValue, String label, int value) {
        Row row = Row.of(orderValue, TopNOpaquePayloadTypeParityTest.payload(label, value));
        row.setKind(kind);
        return row;
    }

    private static List<String> collectOrdered(TableResult result) throws Exception {
        try (CloseableIterator<Row> rows = result.collect()) {
            List<String> output = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                output.add(row.toString());
            }
            return output;
        }
    }

    private static List<String> strictFloatingOrder(List<String> rows) {
        return rows.stream()
                .filter(row -> !row.contains("NaN"))
                .map(row -> row.replace("-0.0", "0.0"))
                .collect(java.util.stream.Collectors.toList());
    }
}
