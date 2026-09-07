/* Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0 */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.BigIntType;
import tech.streamfusion.flink.aggregate.StreamFusionLocalGroupAggregateTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.proto.plan.v1.*;

/** Production local fragment surrounded by ordinary payload-only Calc fragments. */
final class LocalAggregateFixtures {
    private LocalAggregateFixtures() {}

    static Configuration config(long size) {
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, size);
        return config;
    }

    static AggregateCall[] calls() {
        var types = new FlinkTypeFactory(LocalAggregateFixtures.class.getClassLoader(), RelDataTypeSystem.DEFAULT);
        var functions = List.of(
                SqlStdOperatorTable.COUNT, SqlStdOperatorTable.SUM, SqlStdOperatorTable.MIN, SqlStdOperatorTable.MAX);
        var calls = new AggregateCall[4];
        for (int i = 0; i < calls.length; i++)
            calls[i] = AggregateCall.create(
                    functions.get(i),
                    false,
                    i == 0 ? List.of() : List.of(1),
                    -1,
                    types.createFieldTypeFromLogicalType(new BigIntType(i != 0)),
                    "call" + i);
        return calls;
    }

    static byte[] plan(int trigger) {
        byte[] local = StreamFusionLocalGroupAggregateTranslator.createStagePlan(
                SharedAggregateFlinkOracle.INPUT,
                GlobalPartialFixtures.PARTIAL,
                new int[] {0},
                calls(),
                new boolean[] {true, true, true, true},
                true,
                config(trigger));
        return StreamFusionNativeRegionTranslator.compose(List.of(
                StreamFusionNativeRegionTranslator.identifyStage(calc(), 2),
                StreamFusionNativeRegionTranslator.identifyStage(local, 3),
                StreamFusionNativeRegionTranslator.identifyStage(calc(), 4)));
    }

    private static byte[] calc() {
        var calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.getDefaultInstance()))
                .setPreserveInputEnvelope(true);
        for (int i = 0; i < 2; i++)
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(i)));
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }
}
