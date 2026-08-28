/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class MultisetUnnestParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of(multisetOf("repeat", 2, "once", 1, "zero", 0)),
            Row.of(multisetOf("unicode-你好", 2)),
            Row.of(new LinkedHashMap<>()),
            Row.of((Object) null));

    @Test
    void nativeMultisetUnnestRepeatsElementsAndAssignsExpandedOrdinality() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM multiset_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx)",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MULTISET(DataTypes.STRING().notNull()),
                INPUTS,
                "multiset_unnest_input");

        assertNativeExecution();
    }

    @Test
    void nativeLeftMultisetUnnestNullExtendsNullAndEmptyInputs() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM left_multiset_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MULTISET(DataTypes.STRING().notNull()),
                INPUTS,
                "left_multiset_unnest_input");

        assertNativeExecution();
    }

    @Test
    void nativeMultisetUnnestRepeatsAndFlattensNonNullRowElements() throws Exception {
        LinkedHashMap<Row, Integer> populated = new LinkedHashMap<>();
        populated.put(Row.of("repeat", 2), 2);
        populated.put(Row.of("nullable", null), 1);
        populated.put(Row.of("zero", 0), 0);
        java.util.List<Row> inputs =
                Arrays.asList(Row.of(populated), Row.of(new LinkedHashMap<>()), Row.of((Object) null));
        org.apache.flink.api.common.typeinfo.TypeInformation<java.util.Map<Row, Integer>> externalType =
                Types.MAP(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT), Types.INT);
        org.apache.flink.table.types.DataType logicalType = DataTypes.MULTISET(
                DataTypes.ROW(DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))
                        .notNull());

        assertDataStreamParity(
                "SELECT label, amount, ord_idx FROM row_multiset_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(label, amount, ord_idx)",
                externalType,
                logicalType,
                inputs,
                "row_multiset_unnest_input");

        assertDataStreamParity(
                "SELECT label, amount, ord_idx FROM left_row_multiset_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(label, amount, ord_idx) ON TRUE",
                externalType,
                logicalType,
                inputs,
                "left_row_multiset_unnest_input");

        assertNativeExecution();
    }

    @Test
    void nativeMultisetUnnestRepeatsScalarArrayElementsAsWholeValues() throws Exception {
        LinkedHashMap<Integer[], Integer> populated = new LinkedHashMap<>();
        populated.put(new Integer[] {1, null, 3}, 2);
        populated.put(new Integer[] {}, 1);
        java.util.List<Row> inputs =
                Arrays.asList(Row.of(populated), Row.of(new LinkedHashMap<>()), Row.of((Object) null));
        org.apache.flink.api.common.typeinfo.TypeInformation<java.util.Map<Integer[], Integer>> externalType =
                Types.MAP(Types.OBJECT_ARRAY(Types.INT), Types.INT);
        org.apache.flink.table.types.DataType logicalType =
                DataTypes.MULTISET(DataTypes.ARRAY(DataTypes.INT()).notNull());

        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_multiset_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx)",
                externalType,
                logicalType,
                inputs,
                "array_multiset_unnest_input");

        assertDataStreamParity(
                "SELECT item, ord_idx FROM left_array_multiset_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                externalType,
                logicalType,
                inputs,
                "left_array_multiset_unnest_input");

        assertNativeExecution();
    }

    private static LinkedHashMap<String, Integer> multisetOf(Object... entries) {
        LinkedHashMap<String, Integer> multiset = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            multiset.put((String) entries[index], (Integer) entries[index + 1]);
        }
        return multiset;
    }

    private static void assertNativeExecution() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}
