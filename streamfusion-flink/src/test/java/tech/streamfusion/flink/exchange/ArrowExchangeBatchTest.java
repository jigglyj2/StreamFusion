/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

class ArrowExchangeBatchTest {
    @Test
    void transposesValuesAndTheCompleteFlinkRecordEnvelope() {
        GenericRowData insert = GenericRowData.of(10);
        insert.setRowKind(RowKind.INSERT);
        GenericRowData delete = GenericRowData.of(20);
        delete.setRowKind(RowKind.DELETE);
        RowType rowType = RowType.of(new IntType());
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(insert, delete), rowType, allocator)
                        .withEnvelope(
                                new RowKind[] {RowKind.INSERT, RowKind.DELETE},
                                new boolean[] {true, false},
                                new long[] {123, 0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, rowType)) {
            ArrowRowDataBatch batch = envelope.batch();
            assertThat(batch.root().getVector(0).getDataBuffer().memoryAddress())
                    .isEqualTo(input.root().getVector(0).getDataBuffer().memoryAddress());
            assertThat(((IntVector) batch.root().getVector(0)).get(0)).isEqualTo(10);
            assertThat(((IntVector) batch.root().getVector(0)).get(1)).isEqualTo(20);
            TinyIntVector rowKinds = (TinyIntVector) batch.root().getVector(ArrowExchangeBatch.ROW_KIND_COLUMN);
            assertThat(rowKinds.get(0)).isEqualTo(RowKind.INSERT.toByteValue());
            assertThat(rowKinds.get(1)).isEqualTo(RowKind.DELETE.toByteValue());
            BigIntVector timestamps = (BigIntVector) batch.root().getVector(ArrowExchangeBatch.TIMESTAMP_COLUMN);
            assertThat(timestamps.get(0)).isEqualTo(123L);
            assertThat(timestamps.isNull(1)).isTrue();
        }
    }
}
