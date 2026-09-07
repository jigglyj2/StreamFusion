/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.flink.aggregate.StreamFusionGlobalGroupAggregateTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LocalGroupAggregate;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Opaque input fixtures only: retained local API use does not establish fused local production. */
final class GlobalPartialFixtures {
    static final RowType PARTIAL = RowType.of(
            new LogicalType[] {new VarCharType(), new VarBinaryType(false, VarBinaryType.MAX_LENGTH)},
            new String[] {"k", "partial"});

    private GlobalPartialFixtures() {}

    static byte[] localPlan() throws Exception {
        var raw = NativePlan.parseFrom(GeneratedRawMiniBatchParityTest.plan(1))
                .getRoot()
                .getGroupAggregate();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setLocalGroupAggregate(LocalGroupAggregate.newBuilder()
                                .setInput(Operator.newBuilder().setInput(Input.getDefaultInstance()))
                                .addAllGroupingIndices(raw.getGroupingIndicesList())
                                .addAllAggregateCalls(raw.getAggregateCallsList())
                                .setInputChangelog(true)
                                .setMiniBatchSize(1)
                                .setInputSchema(raw.getInputSchema())
                                .setOutputSchema(partialSchema())))
                .build()
                .toByteArray();
    }

    static byte[] globalPlan(int trigger) throws Exception {
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, (long) trigger);
        var types = new FlinkTypeFactory(GlobalPartialFixtures.class.getClassLoader(), RelDataTypeSystem.DEFAULT);
        var calls = new AggregateCall[4];
        var functions = List.of(
                SqlStdOperatorTable.COUNT, SqlStdOperatorTable.SUM, SqlStdOperatorTable.MIN, SqlStdOperatorTable.MAX);
        for (int index = 0; index < calls.length; index++) {
            calls[index] = AggregateCall.create(
                    functions.get(index),
                    false,
                    index == 0 ? List.of() : List.of(1),
                    -1,
                    types.createFieldTypeFromLogicalType(new BigIntType(index != 0)),
                    "call" + index);
        }
        var fragment = NativePlan.parseFrom(StreamFusionGlobalGroupAggregateTranslator.createStagePlan(
                SharedAggregateFlinkOracle.INPUT,
                PARTIAL,
                SharedAggregateFlinkOracle.OUTPUT,
                1,
                calls,
                new boolean[] {true, true, true, true},
                true,
                true,
                0,
                config));
        var plan = NativePlan.parseFrom(SharedMiniBatchControlTest.plan(trigger));
        var root = plan.getRoot();
        var node = root.getCalc().getInput();
        var global = fragment.getRoot().getGlobalGroupAggregate().toBuilder()
                .setInput(node.getGroupAggregate().getInput());
        return plan.toBuilder()
                .setRoot(root.toBuilder()
                        .setCalc(root.getCalc().toBuilder()
                                .setInput(node.toBuilder().setGlobalGroupAggregate(global))))
                .build()
                .toByteArray();
    }

    private static tech.streamfusion.proto.plan.v1.Schema partialSchema() {
        var schema = tech.streamfusion.proto.plan.v1.Schema.newBuilder();
        for (var field : PARTIAL.getFields())
            schema.addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        return schema.build();
    }
}
