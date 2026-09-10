/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner.window;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativePlan;

class WindowJoinStageTranslationTest {
    private static final RowType TYPE = RowType.of(new BigIntType(), new BigIntType(false));
    private static final WindowAttachedWindowingStrategy WINDOW = new WindowAttachedWindowingStrategy(
            new TumblingWindowSpec(Duration.ofSeconds(1), null), new TimestampType(false, TimestampKind.ROWTIME, 3), 1);

    @Test
    void buildsTwoInputFragmentForTheCommonRegionIncludingKeylessWindows() throws Exception {
        for (boolean keyed : new boolean[] {false, true}) {
            var spec = new JoinSpec(
                    FlinkJoinType.INNER,
                    keyed ? new int[] {0} : new int[0],
                    keyed ? new int[] {0} : new int[0],
                    keyed ? new boolean[] {true} : new boolean[0],
                    null);
            var plan = NativePlan.parseFrom(StreamFusionWindowJoinTranslator.createStagePlan(
                    TYPE,
                    TYPE,
                    RowType.of(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()),
                    spec,
                    WINDOW,
                    WINDOW,
                    new Configuration()));
            assertThat(plan.getProtocolVersion()).isEqualTo(3);
            assertThat(plan.getRoot().getClearRecordTimestamps()).isTrue();
            assertThat(plan.getRoot().getWindowJoin().getLeftInput().getInput().getInputIndex())
                    .isZero();
            assertThat(plan.getRoot().getWindowJoin().getRightInput().getInput().getInputIndex())
                    .isEqualTo(1);
        }
    }

    @Test
    void rejectsUnboundedPayloadAndPredicateBeforeRuntimeCreation() {
        var collection = RowType.of(new BigIntType(), new BigIntType(false), new ArrayType(new BigIntType()));
        var spec = new JoinSpec(FlinkJoinType.INNER, new int[] {0}, new int[] {0}, new boolean[] {true}, null);
        assertThatThrownBy(() -> StreamFusionWindowJoinTranslator.createStagePlan(
                        collection, TYPE, TYPE, spec, WINDOW, WINDOW, new Configuration()))
                .hasMessageContaining("scalar payloads");
        var types = new JavaTypeFactoryImpl();
        var rex = new RexBuilder(types);
        RexNode left = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);
        RexNode right = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 2);
        assertThat(WindowJoinComputeSupport.boundedPredicate(
                        rex.makeCall(SqlStdOperatorTable.GREATER_THAN, left, right)))
                .isTrue();
        assertThat(WindowJoinComputeSupport.boundedPredicate(rex.makeCall(
                        SqlStdOperatorTable.GREATER_THAN, rex.makeCall(SqlStdOperatorTable.ABS, left), right)))
                .isFalse();
        assertThat(WindowJoinComputeSupport.inputReason(TYPE, 7)).contains("outside input");
    }
}
