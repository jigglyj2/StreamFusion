/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.IOUtils;
import org.junit.jupiter.api.Test;

class NativePlanClockInputsTest {
    private static final RowType TYPE = RowType.of(new BigIntType());

    @Test
    void directAndDecodedInputsExportClocksInBoundPortOrderWithoutPayloadCopies() throws Exception {
        for (boolean decoded : new boolean[] {false, true}) {
            var readings = new AtomicLong(9998);
            var edge = new NativePlanInputs(true, List.of(0, 1), readings::getAndIncrement);
            try (var allocator = new RootAllocator(1 << 20)) {
                try (var empty = ArrowRowDataBatch.empty(TYPE, allocator);
                        var data = ArrowRowDataBatch.transpose(
                                List.of(GenericRowData.of(7L), GenericRowData.of(9L)), TYPE, allocator)) {
                    var inputs = decoded ? List.of(empty, empty) : List.of(empty, data);
                    try (var prepared = edge.prepare(inputs, decoded ? 1 : -1, decoded ? 2 : 0)) {
                        assertThat(prepared.arrayAddresses).hasSize(4);
                        try (var firstClock = read(allocator, prepared, 2);
                                var secondClock = read(allocator, prepared, 3);
                                var payload = read(allocator, prepared, 1)) {
                            assertThat(firstClock.getRowCount()).isZero();
                            assertThat(secondClock.getRowCount()).isEqualTo(2);
                            assertThat(((BigIntVector) secondClock.getVector(0)).get(0))
                                    .isEqualTo(9998);
                            assertThat(((BigIntVector) secondClock.getVector(0)).get(1))
                                    .isEqualTo(9999);
                            assertThat(payload.getRowCount()).isEqualTo(decoded ? 0 : 2);
                            if (!decoded)
                                assertThat(payload.getVector(0).getDataBufferAddress())
                                        .isEqualTo(data.root().getVector(0).getDataBufferAddress());
                            // Producer release cannot invalidate the consumer's imported Arrow buffers.
                            IOUtils.closeAll(prepared.transferToOutput());
                            assertThat(((BigIntVector) secondClock.getVector(0)).get(1))
                                    .isEqualTo(9999);
                        }
                    }
                    // Control callbacks and inactive channels use empty clock vectors and no clock reads.
                    try (var prepared = edge.prepare(List.of(empty, empty))) {
                        assertThat(prepared.arrayAddresses[0]).isZero();
                        assertThat(prepared.arrayAddresses[1]).isZero();
                        try (var first = read(allocator, prepared, 2);
                                var second = read(allocator, prepared, 3)) {
                            assertThat(first.getRowCount()).isZero();
                            assertThat(second.getRowCount()).isZero();
                        }
                        IOUtils.closeAll(prepared.transferToOutput());
                    }
                    assertThat(readings.get()).isEqualTo(10000);
                }
                assertThat(allocator.getAllocatedMemory()).isZero();
            }
        }
    }

    @Test
    void captureFailureReleasesEarlierExportsAndDoesNotCommitSchemaNegotiation() {
        try (var allocator = new RootAllocator(1 << 20);
                var data = ArrowRowDataBatch.transpose(List.of(GenericRowData.of(7L)), TYPE, allocator)) {
            long before = allocator.getAllocatedMemory();
            var fail = new java.util.concurrent.atomic.AtomicBoolean(true);
            var edge = new NativePlanInputs(true, List.of(0), () -> {
                if (fail.get()) throw new IllegalStateException("clock failed");
                return 17;
            });
            assertThatThrownBy(() -> edge.prepare(List.of(data))).hasMessageContaining("clock failed");
            assertThat(allocator.getAllocatedMemory()).isEqualTo(before);
            fail.set(false);
            try (var prepared = edge.prepare(List.of(data))) {
                assertThat(prepared.schemaAddresses[0]).isNotZero();
                assertThat(prepared.schemaAddresses[1]).isNotZero();
            }
            assertThat(allocator.getAllocatedMemory()).isEqualTo(before);
        }
    }

    private static VectorSchemaRoot read(RootAllocator allocator, NativePlanInputs.Prepared input, int slot) {
        return Data.importVectorSchemaRoot(
                allocator,
                ArrowArray.wrap(input.arrayAddresses[slot]),
                ArrowSchema.wrap(input.schemaAddresses[slot]),
                null,
                false);
    }
}
