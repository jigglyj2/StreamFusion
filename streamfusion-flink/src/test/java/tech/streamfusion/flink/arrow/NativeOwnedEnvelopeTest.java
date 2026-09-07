/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeOwnedEnvelopeTest {
    private static final RowType TYPE = RowType.of(new VarCharType());

    @Test
    void detachedResultsOwnTheirKindsAndTimestampsWithoutAnInputOrPayloadCopy() {
        try (var allocator = new RootAllocator(1 << 20)) {
            for (boolean empty : List.of(false, true)) {
                var root = root(allocator, NativePlanOutputEnvelope.OWNED_TIMESTAMP_V1);
                if (empty) root.setRowCount(0);
                long address = root.getVector(0).getDataBuffer().memoryAddress();
                try (var result = NativePlanOutputEnvelope.read(root, TYPE, allocator);
                        var unrelated = ArrowRowDataBatch.empty(TYPE, allocator)) {
                    var batch = result.selectEnvelopeFrom(List.of());
                    assertThat(batch).isSameAs(result.selectEnvelopeFrom(unrelated));
                    assertThat(batch).isSameAs(result.selectEnvelopeFrom(List.of(unrelated, unrelated)));
                    assertThat(batch.root().getVector(0).getDataBuffer().memoryAddress())
                            .isEqualTo(address);
                    assertThat(batch.size()).isEqualTo(empty ? 0 : 4);
                    long[] times = {Long.MIN_VALUE, 0, Long.MAX_VALUE, 0};
                    for (int row = 0; row < batch.size(); row++) {
                        assertThat(result.inputRow(row)).isEqualTo(-1);
                        assertThat(batch.rowKind(row)).isEqualTo(RowKind.values()[row]);
                        assertThat(batch.hasTimestamp(row)).isEqualTo(row != 3);
                        if (row != 3) assertThat(batch.timestamp(row)).isEqualTo(times[row]);
                    }
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void unknownVersionsAndMixedOwnershipFailAndReleaseVectors() {
        try (var allocator = new RootAllocator(1 << 20)) {
            assertThatThrownBy(() -> NativePlanOutputEnvelope.read(
                            root(allocator, "__streamfusion_owned_timestamp_v99"), TYPE, allocator))
                    .hasMessageContaining("v1");
            assertThat(allocator.getAllocatedMemory()).isZero();
            var invalid = root(allocator, NativePlanOutputEnvelope.OWNED_TIMESTAMP_V1);
            ((IntVector) invalid.getVector(3)).set(2, 0);
            assertThatThrownBy(() -> NativePlanOutputEnvelope.read(invalid, TYPE, allocator))
                    .hasMessageContaining("detached ordinal");
            assertThat(allocator.getAllocatedMemory()).isZero();
            var borrowed = root(allocator, NativePlanOutputEnvelope.OWNED_TIMESTAMP_V1);
            var timestamp = borrowed.getVector(1);
            var withoutTimestamp = borrowed.removeVector(1);
            timestamp.close();
            assertThatThrownBy(() -> NativePlanOutputEnvelope.read(withoutTimestamp, TYPE, allocator))
                    .hasMessageContaining("negative input ordinal");
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static VectorSchemaRoot root(RootAllocator allocator, String timestampName) {
        var payload = new VarCharVector("payload", allocator);
        var timestamps = new BigIntVector(timestampName, allocator);
        var kinds = new TinyIntVector("__streamfusion_row_kind", allocator);
        var ordinals = new IntVector("__streamfusion_input_row", allocator);
        var root = new VectorSchemaRoot(List.of(payload, timestamps, kinds, ordinals));
        root.allocateNew();
        long[] times = {Long.MIN_VALUE, 0, Long.MAX_VALUE, 0};
        for (int row = 0; row < 4; row++) {
            payload.setSafe(row, ("é-" + row).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            kinds.setSafe(row, RowKind.values()[row].toByteValue());
            ordinals.setSafe(row, -1);
            if (row == 3) timestamps.setNull(row);
            else timestamps.setSafe(row, times[row]);
        }
        root.setRowCount(4);
        return root;
    }
}
