/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class StreamFusionStringAggregateAdmissionTest {
    @Test
    void requiresTheSerializedBoundaryAndAppendOnlyStateForBothExtrema() {
        for (var function : List.of(SqlStdOperatorTable.MIN, SqlStdOperatorTable.MAX)) {
            assertThat(StreamFusionPersistentAdmission.unsupportedReason(aggregate(function, true, false), null))
                    .isNull();
            assertThat(StreamFusionPersistentAdmission.unsupportedReason(aggregate(function, false, false), null))
                    .contains("directly consume a HASH exchange", "Java-backed string ordering");
            assertThat(StreamFusionPersistentAdmission.unsupportedReason(aggregate(function, true, true), null))
                    .contains("append-only");
        }
    }

    @Test
    void aCalcAfterTheExchangeDoesNotInheritTheBinaryOrderingProof() {
        var aggregate = aggregate(SqlStdOperatorTable.MAX, true, false);
        var source = aggregate.getInputEdges().get(0).getSource();
        var types = new JavaTypeFactoryImpl();
        var rex = new org.apache.calcite.rex.RexBuilder(types);
        var string = types.createSqlType(SqlTypeName.VARCHAR, 1024);
        var calc = new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                new Configuration(),
                List.of(
                        rex.makeInputRef(string, 0),
                        rex.makeCall(SqlStdOperatorTable.UPPER, rex.makeInputRef(string, 1))),
                null,
                InputProperty.DEFAULT,
                (RowType) source.getOutputType(),
                "string calc");
        calc.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(calc).build()));
        aggregate.setInputEdges(
                List.of(ExecEdge.builder().source(calc).target(aggregate).build()));
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(aggregate, null))
                .contains("directly consume a HASH exchange");
    }

    @Test
    void upstreamJavaAndBinaryComparatorsDifferAcrossTheSupplementaryBoundary() {
        var bmp = BinaryStringData.fromString("\uE000");
        var supplementary = BinaryStringData.fromString("\uD800\uDC00");
        assertThat(bmp.compareTo(supplementary)).isPositive();
        assertThat(BinaryStringData.fromBytes(bmp.toBytes())
                        .compareTo(BinaryStringData.fromBytes(supplementary.toBytes())))
                .isNegative();
        // Materializing bytes alone keeps the Java cache. Admission cannot assume that
        // an arbitrary upstream function has the comparison behavior of a network read.
        assertThat(bmp.compareTo(supplementary)).isPositive();
    }

    private StreamExecGroupAggregate aggregate(SqlAggFunction function, boolean exchange, boolean retractable) {
        var config = new Configuration();
        var input = RowType.of(new VarCharType(), new VarCharType());
        var scan = new StreamExecTableSourceScan(config, null, input, "input");
        scan.setInputEdges(List.of());
        ExecNode<?> source = scan;
        if (exchange) {
            var hash = new StreamExecExchange(
                    config,
                    InputProperty.builder()
                            .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                            .build(),
                    input,
                    "hash exchange");
            hash.setInputEdges(
                    List.of(ExecEdge.builder().source(scan).target(hash).build()));
            source = hash;
        }
        var types = new JavaTypeFactoryImpl();
        var call = AggregateCall.create(
                function, false, List.of(1), -1, types.createSqlType(SqlTypeName.VARCHAR, 1024), "result");
        var aggregate = new StreamExecGroupAggregate(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupAggregate.class),
                config,
                new int[] {0},
                new AggregateCall[] {call},
                new boolean[] {retractable},
                true,
                retractable,
                null,
                List.of(InputProperty.DEFAULT),
                input,
                "aggregate");
        aggregate.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(aggregate).build()));
        return aggregate;
    }
}
