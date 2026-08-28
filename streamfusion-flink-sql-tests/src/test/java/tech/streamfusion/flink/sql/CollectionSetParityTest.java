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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class CollectionSetParityTest extends SqlParityTestSupport {
    @Test
    void arrayExceptMatchesFlinkForPrimitiveElementsAndNullInputs() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_EXCEPT(metric, ARRAY[2, CAST(NULL AS INT), 42]), "
                        + "ARRAY_EXCEPT(metric, CAST(NULL AS ARRAY<INT>)) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, 2, null, 1, 3, 2}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayExceptMatchesFlinkForNestedRows() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_EXCEPT(metric, ARRAY[metric[2]]) FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), Row.of("b", null), Row.of("a", 1), null}),
                        Row.of((Object) new Row[] {Row.of("c", 3)}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayIntersectMatchesFlinkForPrimitiveElementsAndNullInputs() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_INTERSECT(metric, ARRAY_REVERSE(metric)), "
                        + "ARRAY_INTERSECT(metric, CAST(NULL AS ARRAY<INT>)) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, 2, null, 1}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayIntersectMatchesFlinkForNestedRows() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_INTERSECT(metric, ARRAY_REVERSE(metric)) FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), Row.of("b", null), Row.of("a", 1), null}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayUnionMatchesFlinkForPrimitiveElementsAndNullInputs() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_UNION(metric, ARRAY_REVERSE(metric)), "
                        + "ARRAY_UNION(metric, CAST(NULL AS ARRAY<INT>)) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, 2, null, 1}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayUnionMatchesFlinkForNestedRows() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_UNION(metric, ARRAY_REVERSE(metric)) FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), Row.of("b", null), Row.of("a", 1), null}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayDistinctMatchesFlinkForPrimitiveElements() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_DISTINCT(metric) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {null, 1, 2, 1, null, 2}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayDistinctMatchesFlinkForNestedRows() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_DISTINCT(metric) FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), Row.of("b", null), Row.of("a", 1), null, null}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}
