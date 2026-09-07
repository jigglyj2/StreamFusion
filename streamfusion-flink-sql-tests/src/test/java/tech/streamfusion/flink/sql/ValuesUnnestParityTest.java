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

import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ValuesUnnestParityTest extends SqlParityTestSupport {
    @Test
    void sourceFreeArrayConstructorUnnestUsesRetainedExpansion() throws Exception {
        assertParity(
                "SELECT item, ord_idx FROM UNNEST(ARRAY[1, CAST(NULL AS INT), 3]) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx)",
                true);

        SqlArchitectureAssertions.nativeBatchesAtLeast(StreamFusionPlannerFactory.nativePlanBatchCount(), 1);
        SqlArchitectureAssertions.admission();
    }

    @Test
    void sourceFreeMapConstructorUnnestUsesRetainedExpansion() throws Exception {
        assertParity(
                "SELECT map_key, map_value, ord_idx "
                        + "FROM UNNEST(MAP['first', 1, 'nullable', CAST(NULL AS INT)]) "
                        + "WITH ORDINALITY AS expanded(map_key, map_value, ord_idx)",
                true);

        SqlArchitectureAssertions.nativeBatchesAtLeast(StreamFusionPlannerFactory.nativePlanBatchCount(), 1);
        SqlArchitectureAssertions.admission();
    }
}
