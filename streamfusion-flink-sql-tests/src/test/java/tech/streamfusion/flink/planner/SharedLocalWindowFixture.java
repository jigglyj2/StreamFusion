/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;

final class SharedLocalWindowFixture {
    static final RowType INPUT = RowType.of(new BigIntType(), new TimestampType(false, 3));
    static final RowType PARTIAL = RowType.of(
            new BigIntType(),
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new BigIntType(false),
            new BigIntType(false));

    static byte[] plan() throws Exception {
        return plan(INPUT);
    }

    static byte[] plan(RowType input) throws Exception {
        return plan(input, false);
    }

    static byte[] plan(RowType input, boolean tumble) throws Exception {
        return SharedSlicingWindowFixture.compose(
                tech.streamfusion.flink.planner.window.StreamFusionLocalWindowAggregateTranslator.createStagePlan(
                        input,
                        PARTIAL,
                        new int[] {0},
                        new org.apache.calcite.rel.core.AggregateCall[] {
                            SharedSlicingWindowFixture.call(
                                    org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
                                    List.of(),
                                    new BigIntType(false))
                        },
                        new org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy(
                                SharedSlicingWindowFixture.window(tumble),
                                new TimestampType(false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3),
                                1),
                        false,
                        SharedSlicingWindowFixture.config()),
                2,
                4);
    }
}
