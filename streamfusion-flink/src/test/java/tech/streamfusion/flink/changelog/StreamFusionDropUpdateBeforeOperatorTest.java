/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.changelog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class StreamFusionDropUpdateBeforeOperatorTest {
    @Test
    void identifiesOnlyTheArrowRunsThatFlinkWouldRetain() {
        List<RowData> rows = List.of(
                row(1, RowKind.INSERT),
                row(2, RowKind.UPDATE_BEFORE),
                row(3, RowKind.UPDATE_AFTER),
                row(4, RowKind.DELETE));
        RowKind[] kinds = RowKind.values();
        try (RootAllocator allocator = new RootAllocator();
                ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(rows, RowType.of(new IntType(false)), allocator)
                        .withEnvelope(kinds, new boolean[4], new long[4])) {
            assertThat(StreamFusionDropUpdateBeforeOperator.retainedRuns(batch))
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactly(new int[] {0, 1}, new int[] {2, 2});
        }
    }

    private static RowData row(int value, RowKind kind) {
        GenericRowData row = GenericRowData.of(value);
        row.setRowKind(kind);
        return row;
    }
}
