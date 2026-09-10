/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.IntType;

final class SharedSplitIndexMetricFixture extends SharedStringScalarMetricFixture {
    @Override
    List<RexNode> projects(int stage) {
        if (stage == 1) return identity;
        return List.of(rex.makeCall(
                FlinkSqlOperatorTable.SPLIT_INDEX,
                id,
                rex.makeLiteral(stage == 0 ? "/" : "::"),
                rex.makeExactLiteral(
                        java.math.BigDecimal.valueOf(stage == 0 ? 1 : 0),
                        types.createFieldTypeFromLogicalType(new IntType()))));
    }
}
