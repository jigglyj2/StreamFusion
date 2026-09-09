/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.junit.jupiter.api.Test;

class NativeProcessingTimeInputTest {
    @Test
    void generatedClockSequencesPreserveEachFlinkReadingIncludingRollbackAndBoundaries() throws Exception {
        for (int seed = 0; seed < 3; seed++) {
            for (int rows : new int[] {0, 1, 7, 257}) {
                var random = new Random(seed);
                long[] expected = new long[rows];
                var service = new TestProcessingTimeService();
                long[] boundaries = {Long.MIN_VALUE, -1, 0, 9999, 10000, 9998, Long.MAX_VALUE};
                for (int row = 0; row < rows; row++) {
                    service.setCurrentTime(row < boundaries.length ? boundaries[row] : random.nextLong());
                    expected[row] = service.getCurrentProcessingTime();
                }
                var reads = new AtomicInteger();
                try (var allocator = new RootAllocator(1 << 20)) {
                    try (var samples = NativeProcessingTimeInput.capture(
                            rows, () -> expected[reads.getAndIncrement()], allocator)) {
                        var root = samples.root();
                        assertThat(root.getRowCount()).isEqualTo(rows);
                        assertThat(root.getSchema().getFields()).hasSize(1);
                        assertThat(root.getSchema().getFields().get(0).getName())
                                .isEqualTo(NativeProcessingTimeInput.FIELD);
                        assertThat(root.getSchema().getFields().get(0).isNullable())
                                .isFalse();
                        var values = (BigIntVector) root.getVector(0);
                        for (int row = 0; row < rows; row++)
                            assertThat(values.get(row)).isEqualTo(expected[row]);
                        assertThat(values.getNullCount()).isZero();
                        assertThat(reads.get()).isEqualTo(rows);
                    }
                    assertThat(allocator.getAllocatedMemory()).isZero();
                }
            }
        }
    }

    @Test
    void cDataSharesClockBuffersAndKeepsThemAliveAfterProducerCloses() {
        try (var allocator = new RootAllocator(1 << 20)) {
            var samples = NativeProcessingTimeInput.capture(257, () -> 9999, allocator);
            try (var array = ArrowArray.allocateNew(allocator);
                    var schema = ArrowSchema.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, samples.root(), null, array, schema);
                long address = samples.root().getVector(0).getDataBufferAddress();
                try (var imported = Data.importVectorSchemaRoot(allocator, array, schema, null, false)) {
                    assertThat(array.snapshot().release).isZero();
                    assertThat(schema.snapshot().release).isZero();
                    assertThat(imported.getVector(0).getDataBufferAddress()).isEqualTo(address);
                    samples.close();
                    assertThat(((BigIntVector) imported.getVector(0)).get(256)).isEqualTo(9999);
                    assertThat(allocator.getAllocatedMemory()).isPositive();
                }
            } finally {
                samples.close();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void deniedBufferDoesNotReadClockAndMidCaptureFailureReleasesMemory() {
        try (var allocator = new RootAllocator(1024)) {
            var reads = new AtomicInteger();
            assertThatThrownBy(() -> NativeProcessingTimeInput.capture(4096, reads::incrementAndGet, allocator))
                    .isInstanceOf(OutOfMemoryException.class);
            assertThat(reads.get()).isZero();
            assertThat(allocator.getAllocatedMemory()).isZero();
            var failure = new IllegalStateException("clock unavailable");
            assertThatThrownBy(() -> NativeProcessingTimeInput.capture(
                            7,
                            () -> {
                                if (reads.incrementAndGet() == 4) throw failure;
                                return -1;
                            },
                            allocator))
                    .isSameAs(failure);
            assertThat(allocator.getAllocatedMemory()).isZero();
            assertThatThrownBy(() -> NativeProcessingTimeInput.capture(-1, reads::incrementAndGet, allocator))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(reads.get()).isEqualTo(4);
        }
    }
}
