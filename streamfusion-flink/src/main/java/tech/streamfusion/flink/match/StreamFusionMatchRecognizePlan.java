/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.match;

import java.util.List;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.MatchMeasure;
import tech.streamfusion.proto.plan.v1.MatchPatternVariable;
import tech.streamfusion.proto.plan.v1.MatchRecognize;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned protobuf contract for native fixed-sequence MATCH_RECOGNIZE. */
final class StreamFusionMatchRecognizePlan {
    private StreamFusionMatchRecognizePlan() {}

    static byte[] create(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            List<String> variableNames,
            List<Expression> conditions,
            int[] measureVariables,
            int[] measureFields,
            boolean skipPastLastRow) {
        MatchRecognize.Builder match = MatchRecognize.newBuilder()
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType))
                .setSkipPastLastRow(skipPastLastRow);
        for (int key : partitionKeys) {
            match.addPartitionKeyIndices(key);
        }
        for (int index = 0; index < variableNames.size(); index++) {
            MatchPatternVariable.Builder variable =
                    MatchPatternVariable.newBuilder().setName(variableNames.get(index));
            if (conditions.get(index) != null) {
                variable.setCondition(conditions.get(index));
            }
            match.addVariables(variable);
        }
        for (int index = 0; index < measureVariables.length; index++) {
            match.addMeasures(MatchMeasure.newBuilder()
                    .setVariableIndex(measureVariables[index])
                    .setFieldIndex(measureFields[index]));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setMatchRecognize(match))
                .build()
                .toByteArray();
    }

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
    }
}
