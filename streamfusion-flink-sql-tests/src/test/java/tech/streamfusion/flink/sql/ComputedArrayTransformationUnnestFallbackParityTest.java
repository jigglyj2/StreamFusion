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

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ComputedArrayTransformationUnnestFallbackParityTest extends SqlParityTestSupport {
    @Test
    void fallbackUnnestEvaluatesArrayAppendDuringWholePlanFallback() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT item, ord_idx FROM array_append_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_APPEND(metric, 9)) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "array_append_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }

    @Test
    void fallbackUnnestEvaluatesArrayPrependDuringWholePlanFallback() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT item, ord_idx FROM array_prepend_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_PREPEND(metric, CAST(NULL AS INT))) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "array_prepend_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }

    @Test
    void fallbackUnnestEvaluatesVariadicArrayConcatDuringWholePlanFallback() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT item, ord_idx FROM array_concat_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_CONCAT(metric, ARRAY[9, CAST(NULL AS INT)], metric)) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "array_concat_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }

    @Test
    void fallbackUnnestEvaluatesArrayRemoveDuringWholePlanFallback() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT item, ord_idx FROM array_remove_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_REMOVE(metric, 2)) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {2, null, 1, 2}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_remove_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }
}
