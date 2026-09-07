/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativePlanOutputEnvelopeTest {
    @Test
    void onlyActualInputMetadataNamesAreReserved() {
        for (String name : List.of(
                "__streamfusion_row_kind",
                "__streamfusion_input_row_kind",
                "__streamfusion_input_row",
                "__streamfusion_stream_record_timestamp",
                "__streamfusion_key",
                "__streamfusion_routing_key",
                "__streamfusion_owned_timestamp_v2"))
            assertThat(NativePlanOutputEnvelope.isReservedInputField(name))
                    .as(name)
                    .isTrue();
        assertThat(NativePlanOutputEnvelope.isReservedInputField("__streamfusion_accumulator"))
                .isFalse();
        assertThat(NativePlanOutputEnvelope.isReservedInputField("key")).isFalse();
    }

    private static final RowType TYPE = RowType.of(new VarCharType());

    @Test
    void nativeKindsOverrideInputKindsWhileSelectedTimestampsAndPayloadBuffersArePreserved() {
        try (var allocator = new RootAllocator(1L << 20)) {
            try (var input = ArrowRowDataBatch.transpose(
                            List.of(
                                    GenericRowData.of(StringData.fromString("a")),
                                            GenericRowData.of(StringData.fromString("b")),
                                    GenericRowData.of(StringData.fromString("c")),
                                            GenericRowData.of(StringData.fromString("d"))),
                            TYPE,
                            allocator)
                    .withEnvelope(
                            new RowKind[] {RowKind.INSERT, RowKind.INSERT, RowKind.INSERT, RowKind.INSERT},
                            new boolean[] {true, false, true, true},
                            new long[] {10, 0, 30, 40})) {
                var root = root(allocator, "__streamfusion_row_kind");
                long address = root.getVector(0).getDataBuffer().memoryAddress();
                try (var result = NativePlanOutputEnvelope.read(root, TYPE, allocator)) {
                    var output = result.selectEnvelopeFrom(List.of(input));
                    assertThat(output.root().getVector(0).getDataBuffer().memoryAddress())
                            .isEqualTo(address);
                    for (int row = 0; row < 4; row++) {
                        assertThat(output.rowKind(row)).isEqualTo(RowKind.values()[row]);
                        assertThat(output.hasTimestamp(row)).isEqualTo(input.hasTimestamp(3 - row));
                        if (output.hasTimestamp(row))
                            assertThat(output.timestamp(row)).isEqualTo(input.timestamp(3 - row));
                    }
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void malformedEnvelopeIsRejectedAndReleasesAllVectors() {
        try (var allocator = new RootAllocator(1L << 20)) {
            assertThatThrownBy(() ->
                            NativePlanOutputEnvelope.read(root(allocator, "payload_not_metadata"), TYPE, allocator))
                    .hasMessageContaining("RowKind");
            assertThat(allocator.getAllocatedMemory()).isZero();
            var invalid = root(allocator, "__streamfusion_row_kind");
            ((TinyIntVector) invalid.getVector(1)).setNull(1);
            assertThatThrownBy(() -> NativePlanOutputEnvelope.read(invalid, TYPE, allocator))
                    .hasMessageContaining("null metadata");
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static VectorSchemaRoot root(RootAllocator allocator, String kindName) {
        var payload = new VarCharVector("payload", allocator);
        var kinds = new TinyIntVector(kindName, allocator);
        var ordinals = new IntVector("__streamfusion_input_row", allocator);
        var root = new VectorSchemaRoot(List.of(payload, kinds, ordinals));
        root.allocateNew();
        for (int row = 0; row < 4; row++) {
            payload.setSafe(row, ("é-" + row).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            kinds.setSafe(row, RowKind.values()[row].toByteValue());
            ordinals.setSafe(row, 3 - row);
        }
        root.setRowCount(4);
        return root;
    }
}
