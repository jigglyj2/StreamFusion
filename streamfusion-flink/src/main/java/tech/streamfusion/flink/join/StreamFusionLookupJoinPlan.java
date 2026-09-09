/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.join;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LookupJoin;
import tech.streamfusion.proto.plan.v1.LookupJoinKind;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

/** Portable synchronous inner lookup contract. All runtime input/output remains Arrow. */
public final class StreamFusionLookupJoinPlan {
    private StreamFusionLookupJoinPlan() {}

    public static byte[] createStagePlan(RowType input, RowType side, RowType output, int[] probeKeys, int[] sideKeys) {
        var join = LookupJoin.newBuilder()
                .setKind(LookupJoinKind.LOOKUP_JOIN_KIND_INNER)
                .setInputSchema(schema(input))
                .setSideSchema(schema(side))
                .setOutputSchema(schema(output))
                .setInput(Operator.newBuilder().setInput(Input.getDefaultInstance()));
        for (int key : probeKeys) join.addProbeKeys(key);
        for (int key : sideKeys) join.addSideKeys(key);
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder().setLookupJoin(join))
                .build()
                .toByteArray();
    }

    private static Schema schema(RowType type) {
        var result = Schema.newBuilder();
        for (var field : type.getFields())
            result.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        return result.build();
    }
}
