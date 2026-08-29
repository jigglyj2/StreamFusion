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

import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ValuesParityTest extends SqlParityTestSupport {
    private static final String ALL_SCALAR_TYPES = "SELECT * FROM (VALUES ("
            + "CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, CAST(4 AS BIGINT), "
            + "CAST(1.5 AS FLOAT), CAST(2.5 AS DOUBLE), TRUE, CAST('abc' AS CHAR(3)), "
            + "CAST('text' AS VARCHAR(8)), CAST(X'0102' AS BINARY(2)), CAST(X'0304' AS VARBINARY(4)), "
            + "CAST(12.34 AS DECIMAL(10, 2)), DATE '2026-08-28', TIME '12:34:56.123', "
            + "TIMESTAMP '2026-08-28 12:34:56.123'), ("
            + "CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT), CAST(NULL AS INT), CAST(NULL AS BIGINT), "
            + "CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE), CAST(NULL AS BOOLEAN), CAST(NULL AS CHAR(3)), "
            + "CAST(NULL AS VARCHAR(8)), CAST(NULL AS BINARY(2)), CAST(NULL AS VARBINARY(4)), "
            + "CAST(NULL AS DECIMAL(10, 2)), CAST(NULL AS DATE), CAST(NULL AS TIME(3)), "
            + "CAST(NULL AS TIMESTAMP(3)))) AS v(ti, si, i, bi, f, d, b, c, s, fb, vb, decimal_value, dt, tm, ts)";

    @Test
    void sourceFreeValuesMatchFlinkByteForByte() throws Exception {
        assertParity("VALUES (1, 'one'), (2, 'two')", true);

        assertThat(StreamFusionPlannerFactory.nativeValuesBatchCount()).isGreaterThan(0);
    }

    @Test
    void scalarValuesWithPlannerInsertedCalcsStillMatchFlinkByteForByte() throws Exception {
        assertParity(ALL_SCALAR_TYPES, true);
    }
}
