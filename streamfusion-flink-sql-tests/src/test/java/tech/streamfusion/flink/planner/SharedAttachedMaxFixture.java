/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import tech.streamfusion.flink.memory.FlinkOperatorMemoryShare;
import tech.streamfusion.flink.planner.window.StreamFusionLocalWindowAggregateTranslator;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.*;

/** SQL-derived local MAX/COUNT on the second exit of a shared global window. */
final class SharedAttachedMaxFixture {
    static final long BUFFER_BYTES = 3L << 20;
    static final RowType PARTIAL = RowType.of(
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH), new BigIntType(false), new BigIntType(false));
    final OneInputTransformation<?, ?> flink;
    final RowType input;
    final RowType output;
    private final int[] projections;
    final NativeRegionPlan plan;

    @SuppressWarnings("unchecked")
    SharedAttachedMaxFixture(boolean attached) throws Exception {
        flink = SlicingWindowFlinkPlan.stage("LocalWindowAggregate", AttachedSlicingWindowFixture.sql(false));
        input = ((InternalTypeInfo<RowData>) flink.getInputType()).toRowType();
        output = ((InternalTypeInfo<RowData>) flink.getOutputType()).toRowType();
        projections = new int[input.getFieldCount()];
        for (int i = 0; i < projections.length; i++) {
            switch (input.getFieldNames().get(i)) {
                case "n":
                    projections[i] = 1;
                    break;
                case "window_start":
                    projections[i] = attached ? 3 : 2;
                    break;
                case "window_end":
                    projections[i] = attached ? 4 : 3;
                    break;
                default:
                    throw new AssertionError("Unexpected local MAX input " + input);
            }
        }
        var anonymous = Operator.newBuilder().setInput(Input.newBuilder()).build();
        var calc = Calc.newBuilder().setInput(anonymous).setPreserveInputEnvelope(true);
        for (int index : projections)
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(index)));
        var fragment = StreamFusionLocalWindowAggregateTranslator.createStagePlan(
                input,
                PARTIAL,
                new int[0],
                new org.apache.calcite.rel.core.AggregateCall[] {
                    SharedSlicingWindowFixture.call(
                            SqlStdOperatorTable.MAX,
                            List.of(input.getFieldIndex("n")),
                            input.getTypeAt(input.getFieldIndex("n"))),
                    SharedSlicingWindowFixture.call(SqlStdOperatorTable.COUNT, List.of(), new BigIntType(false))
                },
                new org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy(
                        SharedSlicingWindowFixture.hop(),
                        new TimestampType(false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3),
                        input.getFieldIndex("window_end")),
                false,
                SharedSlicingWindowFixture.config());
        var local = NativePlan.parseFrom(fragment).getRoot().toBuilder().setPlanNodeId(5);
        local.getLocalWindowAggregateBuilder().setInput(anonymous);
        plan = SharedWindowMultiOutputFixture.plan(attached).toBuilder()
                .setOutputStageIds(1, 5)
                .setStages(
                        2,
                        NativeRegionStage.newBuilder()
                                .setOperator(
                                        Operator.newBuilder().setPlanNodeId(4).setCalc(calc))
                                .addInputs(
                                        NativeRegionInputReference.newBuilder().setStageId(3)))
                .addStages(NativeRegionStage.newBuilder()
                        .setOperator(local)
                        .addInputs(NativeRegionInputReference.newBuilder().setStageId(4)))
                .build();
    }

    RowData project(RowData row) {
        var projected = new GenericRowData(row.getRowKind(), projections.length);
        for (int i = 0; i < projections.length; i++)
            projected.setField(
                    i,
                    RowData.createFieldGetter(input.getTypeAt(i), projections[i])
                            .getFieldOrNull(row));
        return projected;
    }

    NativeLocalWindowResources resources() {
        // The task harness has 64 MiB; the original local slicer owns three of 64 operator shares.
        return new NativeLocalWindowResources(
                Map.of(5L, new FlinkOperatorMemoryShare(3, 64, Set.of(ManagedMemoryUseCase.OPERATOR))));
    }
}
