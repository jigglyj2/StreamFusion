/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class ArrowExchangeInputBatchTest {
    @Test
    void zeroColumnPayloadRetainsReceivingAllocatorAndAllRecordEnvelopes() {
        try (var allocator = new RootAllocator(Long.MAX_VALUE)) {
            var kinds = new TinyIntVector(ArrowExchangeBatch.ROW_KIND_COLUMN, allocator);
            var timestamps = new BigIntVector(ArrowExchangeBatch.TIMESTAMP_COLUMN, allocator);
            try (var root = new VectorSchemaRoot(List.of(kinds, timestamps))) {
                RowKind[] expected = RowKind.values();
                for (int row = 0; row < expected.length; row++) {
                    kinds.setSafe(row, expected[row].toByteValue());
                    if (row % 2 == 0) timestamps.setSafe(row, 100L + row);
                    else timestamps.setNull(row);
                }
                root.setRowCount(expected.length);
                // The fixture owns root; the receiving view borrows it for these assertions.
                var decoded = new ArrowExchangeInputBatch(root, RowType.of());
                var batch = decoded.arrowBatch();
                assertThat(batch.allocator()).isSameAs(allocator);
                assertThat(batch.size()).isEqualTo(expected.length);
                assertThat(batch.root().getFieldVectors()).isEmpty();
                for (int row = 0; row < expected.length; row++) {
                    assertThat(batch.rowKind(row)).isEqualTo(expected[row]);
                    assertThat(decoded.rowView(row).getRowKind()).isEqualTo(expected[row]);
                    assertThat(batch.hasTimestamp(row)).isEqualTo(row % 2 == 0);
                    if (row % 2 == 0) assertThat(batch.timestamp(row)).isEqualTo(100L + row);
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }
}
