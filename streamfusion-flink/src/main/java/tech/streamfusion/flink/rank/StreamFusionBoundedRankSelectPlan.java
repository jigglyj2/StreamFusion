/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.rank;

import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.TopN;
import tech.streamfusion.proto.plan.v1.TopNRankType;
import tech.streamfusion.proto.plan.v1.TopNStrategy;

/** Builds the bounded, tie-aware Top-N contract used to fuse BatchExecSort into BatchExecRank. */
final class StreamFusionBoundedRankSelectPlan {
    private StreamFusionBoundedRankSelectPlan() {}

    static byte[] create(
            RowType inputType,
            RowType outputType,
            int[] partitionFields,
            int[] sortFields,
            SortSpec inputSortSpec,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber) {
        TopN.Builder rank = TopN.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setRankStart(rankStart)
                .setRankEnd(rankEnd)
                .setOutputRankNumber(outputRankNumber)
                .setGenerateUpdateBefore(false)
                .setStrategy(TopNStrategy.TOP_N_STRATEGY_APPEND_FAST)
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType))
                .setStateTtlMillis(0)
                .setRankType(TopNRankType.TOP_N_RANK_TYPE_RANK)
                .setBoundedFinalOutput(true)
                .setPhysicalInputSemantics(true);
        for (int field : partitionFields) {
            rank.addPartitionKeyIndices(field);
        }
        int searchFrom = partitionFields.length;
        for (int field : sortFields) {
            int specIndex = findField(inputSortSpec, field, searchFrom);
            SortSpec.SortFieldSpec spec = inputSortSpec.getFieldSpec(specIndex);
            rank.addSortKeyIndices(field)
                    .addSortAscending(spec.getIsAscendingOrder())
                    .addSortNullsLast(spec.getNullIsLast());
            searchFrom = specIndex + 1;
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTopN(rank))
                .build()
                .toByteArray();
    }

    private static int findField(SortSpec spec, int field, int from) {
        for (int index = from; index < spec.getFieldSize(); index++) {
            if (spec.getFieldSpec(index).getFieldIndex() == field) {
                return index;
            }
        }
        throw new IllegalArgumentException("Rank field " + field + " is absent from its input sort");
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
