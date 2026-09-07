/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ManagedFilterTypeParityTest extends SqlParityTestSupport {
    @Test
    void filtersEverySupportedPayloadFamilyWithNullablePredicatesAndAllChangelogKinds() throws Exception {
        List<Object[]> types = SelectDistinctFallbackParityTest.distinctTypes()
                .map(arguments -> arguments.get())
                .collect(java.util.stream.Collectors.toList());
        var rows = new ArrayList<Row>();
        for (int seed = 0; seed < 4; seed++) {
            var random = new Random(seed);
            for (int index = 0; index < 48; index++) {
                Object[] values = new Object[types.size() + 1];
                values[0] = index % 7 == 0 ? null : index % 3 == 0;
                for (int field = 0; field < types.size(); field++) {
                    values[field + 1] = random.nextInt(5) == 0 ? null : types.get(field)[3 + random.nextInt(2)];
                }
                // Identical payloads exercise both insertion and retraction without a key rewrite.
                for (RowKind kind : RowKind.values()) rows.add(Row.ofKind(kind, values));
            }
        }
        byte[] expected = execute(types, rows, false);
        byte[] actual = execute(types, rows, true);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        SqlArchitectureAssertions.admission();
    }

    private static byte[] execute(List<Object[]> types, List<Row> rows, boolean nativeEnabled) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (nativeEnabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        var names = new String[types.size() + 1];
        var information = new TypeInformation<?>[names.length];
        var schema = Schema.newBuilder();
        names[0] = "keep_row";
        information[0] = Types.BOOLEAN;
        schema.column(names[0], org.apache.flink.table.api.DataTypes.BOOLEAN());
        for (int index = 0; index < types.size(); index++) {
            names[index + 1] = "c" + index;
            information[index + 1] = (TypeInformation<?>) types.get(index)[1];
            schema.column(names[index + 1], (DataType) types.get(index)[2]);
        }
        tables.createTemporaryView(
                "typed_filter",
                tables.fromChangelogStream(
                        environment.fromCollection(rows, Types.ROW_NAMED(names, information)),
                        schema.build(),
                        ChangelogMode.all()));
        return collect(tables.executeSql("SELECT *, c16 IS NULL AS absent_months, "
                + "c17 IS NULL AS absent_millis, c20 IS NULL AS absent_bag FROM typed_filter WHERE keep_row"));
    }
}
