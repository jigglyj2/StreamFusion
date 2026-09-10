/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class StreamFusionPersistentAdmissionTest {
    private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();

    @Test
    void admitsGeneralKeyedIntegerAggregatesButPreservesUnverifiedSubsets() {
        for (var function : List.of(
                SqlStdOperatorTable.COUNT,
                SqlStdOperatorTable.SUM,
                SqlStdOperatorTable.SUM0,
                SqlStdOperatorTable.MIN,
                SqlStdOperatorTable.MAX,
                SqlStdOperatorTable.AVG)) {
            var aggregate = aggregate(function, false, new BigIntType(), new VarCharType(), true);
            assertThat(StreamFusionPersistentAdmission.unsupportedReason(aggregate, null))
                    .isNull();
            if (function == SqlStdOperatorTable.COUNT)
                assertThat(StreamFusionPersistentAdmission.unsupportedReason(
                                aggregate(function, true, new BigIntType(), new VarCharType(), true), null))
                        .isNull();
            else if (function != SqlStdOperatorTable.MIN && function != SqlStdOperatorTable.MAX)
                assertThat(StreamFusionPersistentAdmission.unsupportedReason(
                                aggregate(function, true, new BigIntType(), new VarCharType(), true), null))
                        .contains("non-DISTINCT");
        }
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(
                        aggregate(SqlStdOperatorTable.SUM, false, new DoubleType(), new VarCharType(), true), null))
                .contains("argument type");
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(
                        aggregate(SqlStdOperatorTable.SUM, false, new BigIntType(), new DoubleType(), true), null))
                .contains("grouping type DOUBLE");
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(
                        aggregate(SqlStdOperatorTable.AVG, false, new BigIntType(), new VarCharType(), false), null))
                .contains("singleton/global");
    }

    @Test
    void activeMiniBatchConfigurationCannotBypassTheBundleGate() {
        var aggregate = aggregate(SqlStdOperatorTable.AVG, false, new BigIntType(), new VarCharType(), true);
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(aggregate, config))
                .contains("mini-batch");
    }

    @Test
    void comparisonsAreBoundedButComputedOperandsKeepTheirWorkspaceGate() {
        var rex = new RexBuilder(types);
        var left = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);
        var right = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 1);
        var comparison = rex.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, left, right);
        assertThat(StreamFusionPersistentAdmission.boundedPredicate(rex.makeCall(
                        SqlStdOperatorTable.AND, comparison, rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, right))))
                .isTrue();
        assertThat(StreamFusionPersistentAdmission.boundedPredicate(rex.makeCall(
                        SqlStdOperatorTable.EQUALS, left, rex.makeCall(SqlStdOperatorTable.PLUS, left, right))))
                .isFalse();
    }

    @Test
    void timestampLiteralOffsetsAreBoundedButUnverifiedArithmeticStaysGated() {
        var types = new org.apache.flink.table.planner.calcite.FlinkTypeFactory(
                getClass().getClassLoader(), org.apache.flink.table.planner.calcite.FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        var interval = rex.makeIntervalLiteral(
                java.math.BigDecimal.valueOf(10000),
                new org.apache.calcite.sql.SqlIntervalQualifier(
                        org.apache.calcite.avatica.util.TimeUnit.SECOND,
                        null,
                        org.apache.calcite.sql.parser.SqlParserPos.ZERO));
        for (int precision : new int[] {0, 3, 6, 9}) {
            var timestamp = rex.makeInputRef(types.createSqlType(SqlTypeName.TIMESTAMP, precision), 0);
            for (var operator : List.of(SqlStdOperatorTable.PLUS, SqlStdOperatorTable.MINUS)) {
                var offset = rex.makeCall(operator, timestamp, interval);
                assertThat(StreamFusionPersistentAdmission.boundedPredicate(
                                rex.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, timestamp, offset)))
                        .isEqualTo(precision == 3);
                assertThat(StreamFusionPersistentAdmission.boundedPredicate(
                                rex.makeCall(SqlStdOperatorTable.IS_NULL, rex.makeCall(operator, offset, interval))))
                        .isFalse();
                assertThat(StreamFusionPersistentAdmission.boundedPredicate(rex.makeCall(
                                SqlStdOperatorTable.IS_NULL,
                                rex.makeCall(operator, timestamp, rex.makeInputRef(interval.getType(), 1)))))
                        .isFalse();
                assertThat(StreamFusionPersistentAdmission.boundedPredicate(rex.makeCall(
                                SqlStdOperatorTable.IS_NULL,
                                rex.makeCall(operator, timestamp, rex.makeNullLiteral(interval.getType())))))
                        .isFalse();
            }
        }
        var timestamp = rex.makeInputRef(types.createSqlType(SqlTypeName.TIMESTAMP, 3), 0);
        var month = rex.makeIntervalLiteral(
                java.math.BigDecimal.ONE,
                new org.apache.calcite.sql.SqlIntervalQualifier(
                        org.apache.calcite.avatica.util.TimeUnit.MONTH,
                        null,
                        org.apache.calcite.sql.parser.SqlParserPos.ZERO));
        assertThat(StreamFusionPersistentAdmission.boundedPredicate(rex.makeCall(
                        SqlStdOperatorTable.IS_NULL, rex.makeCall(SqlStdOperatorTable.PLUS, timestamp, month))))
                .isFalse();
    }

    private StreamExecGroupAggregate aggregate(
            SqlAggFunction function, boolean distinct, LogicalType value, LogicalType key, boolean keyed) {
        var config = new Configuration();
        var input = RowType.of(key, value);
        var source = new StreamExecTableSourceScan(config, null, input, "input");
        source.setInputEdges(List.of());
        var call = AggregateCall.create(
                function, distinct, List.of(1), -1, types.createSqlType(SqlTypeName.BIGINT), "result");
        var aggregate = new StreamExecGroupAggregate(
                org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext.newNodeId(),
                org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext.newContext(
                        StreamExecGroupAggregate.class),
                config,
                keyed ? new int[] {0} : new int[0],
                new AggregateCall[] {call},
                new boolean[] {true},
                true,
                true,
                null,
                List.of(InputProperty.DEFAULT),
                keyed ? RowType.of(key, new BigIntType()) : RowType.of(new BigIntType()),
                "aggregate");
        aggregate.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(aggregate).build()));
        return aggregate;
    }
}
