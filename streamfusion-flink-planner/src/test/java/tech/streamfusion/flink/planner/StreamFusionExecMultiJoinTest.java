/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class StreamFusionExecMultiJoinTest {
    @Test
    void lowersBinaryMultiJoinWithResidualPredicateToRegularJoin() throws Exception {
        Configuration configuration = new Configuration();
        RowType inputType =
                RowType.of(new LogicalType[] {new BigIntType(false), new VarCharType()}, new String[] {"id", "value"});
        RowType outputType = RowType.of(
                new LogicalType[] {new BigIntType(false), new VarCharType(), new BigIntType(false), new VarCharType()},
                new String[] {"left_id", "left_value", "right_id", "right_value"});
        StreamExecTableSourceScan left = source(configuration, inputType, "left");
        StreamExecTableSourceScan right = source(configuration, inputType, "right");

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rex = new RexBuilder(typeFactory);
        RexNode equi = rex.makeCall(
                SqlStdOperatorTable.EQUALS,
                rex.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0),
                rex.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 2));
        RexNode residual = rex.makeCall(
                SqlStdOperatorTable.NOT_EQUALS,
                rex.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
                rex.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 3));
        RexNode condition = rex.makeCall(SqlStdOperatorTable.AND, equi, residual);
        StreamExecMultiJoin join = new StreamExecMultiJoin(
                configuration,
                List.of(FlinkJoinType.INNER, FlinkJoinType.INNER),
                Arrays.asList(null, condition),
                condition,
                Map.of(1, List.of(new ConditionAttributeRef(0, 0, 1, 0))),
                List.of(List.of(), List.of()),
                Map.of(),
                List.of(InputProperty.DEFAULT, InputProperty.DEFAULT),
                outputType,
                "binary residual multi-join");
        join.setInputEdges(List.of(
                ExecEdge.builder().source(left).target(join).build(),
                ExecEdge.builder().source(right).target(join).build()));

        ExecNode<?> replacement = new StreamFusionExecGraphProcessor().convert(join);

        assertThat(replacement).isInstanceOf(StreamFusionExecRegularJoin.class);
        Field joinSpecField = StreamFusionExecRegularJoin.class.getDeclaredField("joinSpec");
        joinSpecField.setAccessible(true);
        JoinSpec joinSpec = (JoinSpec) joinSpecField.get(replacement);
        assertThat(joinSpec.getLeftKeys()).containsExactly(0);
        assertThat(joinSpec.getRightKeys()).containsExactly(0);
        assertThat(joinSpec.getNonEquiCondition()).contains(condition);
        assertThat(replacement.getInputEdges()).extracting(ExecEdge::getSource).containsExactly(left, right);
    }

    private static StreamExecTableSourceScan source(Configuration configuration, RowType type, String name) {
        StreamExecTableSourceScan source = new StreamExecTableSourceScan(configuration, null, type, name);
        source.setInputEdges(List.of());
        return source;
    }
}
