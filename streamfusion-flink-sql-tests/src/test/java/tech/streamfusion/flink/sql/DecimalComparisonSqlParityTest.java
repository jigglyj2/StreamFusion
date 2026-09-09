/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class DecimalComparisonSqlParityTest extends SqlParityTestSupport {
    @Test
    void upstreamExactComparisonCasesMatchThroughTheSqlHarness() throws Exception {
        // Exact-number subset of Flink 2.3 DecimalTypeTest.testComparison.
        // Keep inputs as source fields so comparison is evaluated during execution.
        var fields = new String[] {"f63", "f64", "f65", "f67", "f68", "f69"};
        var type = Types.ROW_NAMED(
                fields, Types.BIG_DEC, Types.BIG_DEC, Types.INT, Types.BIG_DEC, Types.BIG_DEC, Types.INT);
        var logical = DataTypes.ROW(
                DataTypes.FIELD("f63", DataTypes.DECIMAL(8, 2)),
                DataTypes.FIELD("f64", DataTypes.DECIMAL(8, 4)),
                DataTypes.FIELD("f65", DataTypes.INT()),
                DataTypes.FIELD("f67", DataTypes.DECIMAL(1, 0)),
                DataTypes.FIELD("f68", DataTypes.DECIMAL(2, 0)),
                DataTypes.FIELD("f69", DataTypes.INT()));
        var rows =
                List.of(Row.of(Row.of(BigDecimal.ONE, BigDecimal.ONE, 1, BigDecimal.ONE, BigDecimal.valueOf(99), 99)));
        String comparisons = "f63 < f64, f63 < f65, f64 < f63, f65 < f63, "
                + "f67 < f68, f67 < f69, f68 < f67, f69 < f67, "
                + "f63 BETWEEN f64 AND 1, f64 BETWEEN f63 AND 1, "
                + "f63 BETWEEN f65 AND 1, f65 BETWEEN f63 AND 1, "
                + "f63 BETWEEN 0 AND f64, f64 BETWEEN 0 AND f63, "
                + "f63 BETWEEN 0 AND f65, f65 BETWEEN 0 AND f63";
        assertDataStreamParity(
                "SELECT " + comparisons.replaceAll("f6[3-9]", "metric.$0") + " FROM decimal_input",
                type,
                logical,
                rows,
                "decimal_input");
    }

    @Test
    void decimalFloatingComparisonRemainsOnWholePlanFallback() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT metric.d < metric.f FROM decimal_input",
                Types.ROW_NAMED(new String[] {"d", "f"}, Types.BIG_DEC, Types.DOUBLE),
                DataTypes.ROW(
                        DataTypes.FIELD("d", DataTypes.DECIMAL(38, 38)), DataTypes.FIELD("f", DataTypes.DOUBLE())),
                List.of(
                        Row.of(Row.of(new BigDecimal("0.00000000000000000000000000000000000001"), 0.0)),
                        Row.of(Row.of(BigDecimal.ZERO, Double.NaN)),
                        Row.of(Row.of(null, 1.0))),
                "decimal_input");
    }
}
