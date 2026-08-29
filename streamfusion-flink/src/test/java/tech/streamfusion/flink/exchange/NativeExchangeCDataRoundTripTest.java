/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class NativeExchangeCDataRoundTripTest {
    @Test
    void preservesRowsChangelogAndTimestampsAcrossNativeFrames() {
        RowType rowType = RowType.of(new IntType(false));
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128);
        GenericRowData insert = GenericRowData.of(1);
        insert.setRowKind(RowKind.INSERT);
        GenericRowData delete = GenericRowData.of(42);
        delete.setRowKind(RowKind.DELETE);
        List<ResultRow> output = new ArrayList<>();
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(insert, delete), rowType, allocator)
                        .withEnvelope(
                                new RowKind[] {RowKind.INSERT, RowKind.DELETE},
                                new boolean[] {true, false},
                                new long[] {100, 0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, rowType)) {
            List<NativeExchangeFrame> frames = ArrowExchangeCDataBridge.route(plan, envelope.batch(), allocator);
            for (NativeExchangeFrame frame : frames) {
                try (ArrowExchangeInputBatch decoded =
                        ArrowExchangeInputCDataBridge.decode(plan, frame, rowType, allocator)) {
                    for (int row = 0; row < decoded.size(); row++) {
                        output.add(new ResultRow(
                                decoded.rowView(row).getInt(0),
                                decoded.rowView(row).getRowKind(),
                                decoded.hasTimestamp(row),
                                decoded.timestamp(row)));
                    }
                }
            }
        }
        output.sort(Comparator.comparingInt(result -> result.value));

        assertThat(output)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        new ResultRow(1, RowKind.INSERT, true, 100L),
                        new ResultRow(42, RowKind.DELETE, false, Long.MIN_VALUE));
    }

    private static final class ResultRow {
        private final int value;
        private final RowKind kind;
        private final boolean hasTimestamp;
        private final long timestamp;

        private ResultRow(int value, RowKind kind, boolean hasTimestamp, long timestamp) {
            this.value = value;
            this.kind = kind;
            this.hasTimestamp = hasTimestamp;
            this.timestamp = timestamp;
        }
    }
}
