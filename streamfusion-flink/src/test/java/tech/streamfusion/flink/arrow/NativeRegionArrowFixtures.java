/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.proto.plan.v1.*;

final class NativeRegionArrowFixtures {
    static final RowType TYPE = RowType.of(new IntType(), new VarCharType(), new ArrayType(new IntType()));
    static final RowType TEXT = RowType.of(new VarCharType());

    static byte[] plan() {
        var region = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(1)
                .addOutputStageIds(11)
                .addOutputStageIds(13);
        for (int index = 0; index < 3; index++) {
            var calc = Calc.newBuilder()
                    .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                    .setPreserveInputEnvelope(true);
            int[] columns = index == 0 ? new int[] {0, 1, 2} : index == 1 ? new int[] {1} : new int[] {0};
            for (int column : columns)
                calc.addProjections(Expression.newBuilder()
                        .setInputReference(InputReference.newBuilder().setIndex(column)));
            var reference = NativeRegionInputReference.newBuilder();
            if (index == 0) reference.setExternalInput(0);
            else reference.setStageId(10 + index);
            region.addStages(NativeRegionStage.newBuilder()
                    .setOperator(Operator.newBuilder().setPlanNodeId(11 + index).setCalc(calc))
                    .addInputs(reference));
        }
        return region.build().toByteArray();
    }

    static ArrowRowDataBatch input(RootAllocator allocator, int seed) {
        var rows = new ArrayList<RowData>();
        RowKind[] kinds = new RowKind[37];
        boolean[] timestamps = new boolean[37];
        long[] times = new long[37];
        for (int index = 0; index < 37; index++) {
            kinds[index] = RowKind.values()[index % 4];
            timestamps[index] = index % 3 != 0;
            times[index] = seed * 1000L + index;
            var row = GenericRowData.of(
                    index % 5 == 0 ? null : index + seed,
                    index % 7 == 0 ? null : StringData.fromString("é-" + seed + "-" + index),
                    new GenericArrayData(new Integer[] {index, null, seed}));
            row.setRowKind(kinds[index]);
            rows.add(row);
        }
        return ArrowRowDataBatch.transpose(rows, TYPE, allocator).withEnvelope(kinds, timestamps, times);
    }
}
