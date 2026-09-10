/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.window;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.RegularJoinType;
import tech.streamfusion.proto.plan.v1.WindowJoin;

class StreamFusionWindowJoinPlanTest {
    private static final RowType TYPE = RowType.of(new BigIntType(), new BigIntType(false));

    @Test
    void serializesPredicateNullPolicyChildrenAndAbsentRecordTimestamp() throws Exception {
        JoinSpec spec =
                new JoinSpec(FlinkJoinType.INNER, new int[] {0}, new int[] {0}, new boolean[] {false}, predicate());
        NativePlan plan =
                NativePlan.parseFrom(StreamFusionWindowJoinPlan.createNativeInner(TYPE, TYPE, spec, 1, 1, "UTC"));
        assertThat(plan.getProtocolVersion()).isEqualTo(3);
        assertThat(plan.getRoot().getClearRecordTimestamps()).isTrue();
        WindowJoin join = plan.getRoot().getWindowJoin();
        assertThat(join.getJoinType()).isEqualTo(RegularJoinType.REGULAR_JOIN_TYPE_INNER);
        assertThat(join.getLeftInput().getInput().getInputIndex()).isZero();
        assertThat(join.getRightInput().getInput().getInputIndex()).isEqualTo(1);
        assertThat(join.getLeftKeyIndicesList()).containsExactly(0);
        assertThat(join.getRightKeyIndicesList()).containsExactly(0);
        assertThat(join.getFilterNullsList()).containsExactly(false);
        assertThat(join.getResidualCondition().getComparison().getOperator())
                .isEqualTo(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL);
        assertThat(join.getResidualCondition()
                        .getComparison()
                        .getLeft()
                        .getInputReference()
                        .getIndex())
                .isZero();
        assertThat(join.getResidualCondition()
                        .getComparison()
                        .getRight()
                        .getInputReference()
                        .getIndex())
                .isEqualTo(2);
    }

    @Test
    void keylessWindowsAndPredicateFreeWindowsHaveExplicitContracts() throws Exception {
        JoinSpec spec = new JoinSpec(FlinkJoinType.INNER, new int[0], new int[0], new boolean[0], null);
        WindowJoin join = NativePlan.parseFrom(
                        StreamFusionWindowJoinPlan.createNativeInner(TYPE, TYPE, spec, 1, 1, "UTC"))
                .getRoot()
                .getWindowJoin();
        assertThat(join.hasLeftInput()).isTrue();
        assertThat(join.hasRightInput()).isTrue();
        assertThat(join.hasResidualCondition()).isFalse();
        assertThat(join.getFilterNullsList()).isEmpty();
    }

    @Test
    void outerSemanticsCannotReuseTheInnerWindowContract() {
        JoinSpec spec = new JoinSpec(FlinkJoinType.LEFT, new int[0], new int[0], new boolean[0], null);
        assertThatThrownBy(() -> StreamFusionWindowJoinPlan.createNativeInner(TYPE, TYPE, spec, 1, 1, "UTC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("INNER");
    }

    @Test
    void legacyStateContractRemainsDistinctFromNativeCompute() throws Exception {
        NativePlan plan = NativePlan.parseFrom(
                StreamFusionWindowJoinPlan.create(TYPE, TYPE, new int[] {0}, new int[] {0}, 1, 1, "UTC"));
        assertThat(plan.getProtocolVersion()).isEqualTo(1);
        assertThat(plan.getRoot().getClearRecordTimestamps()).isFalse();
        assertThat(plan.getRoot().getWindowJoin().hasLeftInput()).isFalse();
        assertThat(plan.getRoot().getWindowJoin().getJoinType())
                .isEqualTo(RegularJoinType.REGULAR_JOIN_TYPE_UNSPECIFIED);
    }

    private static RexNode predicate() {
        JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
        RexBuilder rex = new RexBuilder(types);
        return rex.makeCall(
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0),
                rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 2));
    }
}
