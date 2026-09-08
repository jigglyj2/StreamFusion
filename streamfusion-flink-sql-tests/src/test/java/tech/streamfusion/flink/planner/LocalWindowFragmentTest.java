/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.planner.window.StreamFusionLocalWindowAggregateTranslator;
import tech.streamfusion.proto.plan.v1.NativePlan;

class LocalWindowFragmentTest {
    private static final TimestampType ROWTIME = new TimestampType(false, TimestampKind.ROWTIME, 3);
    private static final RowType OUTPUT = RowType.of(
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH), new BigIntType(false), new BigIntType(false));

    @Test
    void attachedStartIsIgnoredEvenWhenPlannerRetainsItsColumn() throws Exception {
        var input = RowType.of(new BigIntType(false), new TimestampType(true, 3), new TimestampType(false, 3));
        var strategy = new WindowAttachedWindowingStrategy(SharedSlicingWindowFixture.hop(), ROWTIME, 1, 2);
        var fragment = NativePlan.parseFrom(StreamFusionLocalWindowAggregateTranslator.createStagePlan(
                input, OUTPUT, new int[0], count(), strategy, false, SharedSlicingWindowFixture.config()));
        assertThat(fragment.getProtocolVersion()).isEqualTo(2);
        var local = fragment.getRoot().getLocalWindowAggregate();
        assertThat(local.hasAttachedWindowStartIndex()).isFalse();
        assertThat(local.getAttachedWindowEndIndex()).isEqualTo(2);
        assertThat(local.hasAttachedWindowEndIndex()).isTrue();
    }

    @Test
    void rejectsUnverifiedTimeAndRowGeometryBeforeNativeLowering() throws Exception {
        var direct = new TimeAttributeWindowingStrategy(SharedSlicingWindowFixture.hop(), ROWTIME, 1);
        assertThat(StreamFusionLocalWindowAggregateTranslator.unsupportedStageReason(
                        RowType.of(new BigIntType(), new TimestampType(true, 3)),
                        OUTPUT,
                        new int[0],
                        count(),
                        direct,
                        false,
                        SharedSlicingWindowFixture.config()))
                .isNull();
        assertThat(StreamFusionLocalWindowAggregateTranslator.unsupportedStageReason(
                        RowType.of(new BigIntType(), new TimestampType(true, 3)),
                        OUTPUT,
                        new int[0],
                        count(),
                        new WindowAttachedWindowingStrategy(SharedSlicingWindowFixture.hop(), ROWTIME, 1),
                        false,
                        SharedSlicingWindowFixture.config()))
                .contains("non-null attached window end");
        assertThat(StreamFusionLocalWindowAggregateTranslator.unsupportedStageReason(
                        RowType.of(new VarBinaryType(), new TimestampType(false, 3)),
                        OUTPUT,
                        new int[0],
                        count(),
                        direct,
                        false,
                        SharedSlicingWindowFixture.config()))
                .contains("row-size geometry");
        assertThat(StreamFusionLocalWindowAggregateTranslator.unsupportedStageReason(
                        RowType.of(new BigIntType(), new TimestampType(false, 3)),
                        OUTPUT,
                        new int[0],
                        count(),
                        direct,
                        true,
                        SharedSlicingWindowFixture.config()))
                .contains("append-only");
    }

    @Test
    void refusesMalformedPartialSchemasAndUnverifiedAggregates() throws Exception {
        var input = RowType.of(new BigIntType(), new TimestampType(false, 3));
        var direct = new TimeAttributeWindowingStrategy(SharedSlicingWindowFixture.hop(), ROWTIME, 1);
        assertThatThrownBy(() -> StreamFusionLocalWindowAggregateTranslator.createStagePlan(
                        input,
                        RowType.of(new VarBinaryType(), new BigIntType(false), new BigIntType(false)),
                        new int[0],
                        count(),
                        direct,
                        false,
                        SharedSlicingWindowFixture.config()))
                .hasMessageContaining("non-null VARBINARY");
        var sum = new org.apache.calcite.rel.core.AggregateCall[] {
            SharedSlicingWindowFixture.call(SqlStdOperatorTable.SUM, List.of(0), new BigIntType())
        };
        assertThat(StreamFusionLocalWindowAggregateTranslator.unsupportedStageReason(
                        input, OUTPUT, new int[0], sum, direct, false, SharedSlicingWindowFixture.config()))
                .contains("non-DISTINCT COUNT or DataFusion extrema");
    }

    private static org.apache.calcite.rel.core.AggregateCall[] count() {
        return new org.apache.calcite.rel.core.AggregateCall[] {
            SharedSlicingWindowFixture.call(SqlStdOperatorTable.COUNT, List.of(), new BigIntType(false))
        };
    }
}
