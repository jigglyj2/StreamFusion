/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.arrow.NativeRegionArrowFixtures.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;

class ArrowNativeRegionBridgeTest {
    @Test
    void generatedPortChangelogsKeepSchemasAndBufferIdentityAcrossEmptyAndRepeatedInvocations() {
        for (int seed : List.of(3, 19, 71)) {
            var memory = TestingNativeMemoryManager.create();
            try (var allocator = new RootAllocator(64L << 20);
                    var input = input(allocator, seed);
                    var empty = ArrowRowDataBatch.transpose(List.of(), TYPE, allocator);
                    var context = NativeExecutionContext.region(plan(), memory, null, null)) {
                var edge = new ArrowNativeRegionBridge(context, List.of(TYPE, TEXT), allocator);
                long total = 0;
                for (var source : List.of(empty, input, empty, input, input)) {
                    int[] rows = new int[2];
                    try (var stream = edge.executeStream(List.of(source))) {
                        ArrowNativeRegionOutput.Batch next;
                        while ((next = stream.next()) != null) {
                            try (var output = next) {
                                int port = output.port();
                                var batch = output.batch();
                                assertThat(port).isBetween(0, 1);
                                assertThat(batch.size()).isEqualTo(source.size());
                                if (batch.size() > 0) {
                                    assertThat(batch.root()
                                                    .getVector(0)
                                                    .getDataBuffer()
                                                    .memoryAddress())
                                            .isEqualTo(source.root()
                                                    .getVector(port == 0 ? 0 : 1)
                                                    .getDataBuffer()
                                                    .memoryAddress());
                                }
                                var serializer = new RowDataSerializer(port == 0 ? TYPE : TEXT);
                                for (int row = 0; row < batch.size(); row++) {
                                    var expected = source.rowView(row);
                                    if (port == 1)
                                        expected =
                                                GenericRowData.of(expected.isNullAt(1) ? null : expected.getString(1));
                                    var expectedBytes =
                                            serializer.toBinaryRow(expected).copy();
                                    expectedBytes.setRowKind(source.rowKind(row));
                                    var actualBytes = serializer
                                            .toBinaryRow(batch.rowView(row))
                                            .copy();
                                    actualBytes.setRowKind(batch.rowKind(row));
                                    assertThat(actualBytes)
                                            .as("seed=%s port=%s row=%s", seed, port, row)
                                            .isEqualTo(expectedBytes);
                                    assertThat(batch.hasTimestamp(row)).isEqualTo(source.hasTimestamp(row));
                                    if (batch.hasTimestamp(row))
                                        assertThat(batch.timestamp(row)).isEqualTo(source.timestamp(row));
                                }
                                rows[port] += batch.size();
                            }
                        }
                        assertThat(stream.next()).isNull();
                    }
                    assertThat(rows).containsExactly(source.size(), source.size());
                    total += source.size();
                    assertThat(context.metricSnapshot())
                            .containsExactly(11, total, total, 12, total, total, 13, total, total);
                }
                byte[] control = NativeControlInvocation.newBuilder()
                        .setProtocolVersion(1)
                        .build()
                        .toByteArray();
                assertThatThrownBy(() -> edge.executeControlStream(List.of(input), control))
                        .hasMessageContaining("empty");
                drain(edge.executeControlStream(List.of(empty), control));
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    @Test
    void closingInputAndContextHandlesDoesNotInvalidateTheInvocationOrItsReturnedBatches() {
        var memory = TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator(64L << 20);
                var input = input(allocator, 3);
                var context = NativeExecutionContext.region(plan(), memory, null, null)) {
            var edge = new ArrowNativeRegionBridge(context, List.of(TYPE, TEXT), allocator);
            try (var stream = edge.executeStream(List.of(input))) {
                input.close();
                context.close();
                try (var output = stream.next()) {
                    stream.close();
                    stream.close();
                    assertThat(output.batch().size()).isEqualTo(37);
                    assertThat(output.batch().rowView(1).isNullAt(0)).isFalse();
                    assertThatThrownBy(stream::next).hasMessageContaining("closed");
                }
            }
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void admissionAndSchemaFailuresReleaseExportsAndAllowRetryBeforeExecutionStarts() {
        var memory = new DenyingMemory();
        try (var allocator = new RootAllocator(64L << 20);
                var input = input(allocator, 3);
                var context = NativeExecutionContext.region(plan(), memory, null, null)) {
            var edge = new ArrowNativeRegionBridge(context, List.of(TYPE, TEXT), allocator);
            drain(edge.executeStream(List.of(input)));
            long nativeAvailable = memory.available();
            long arrowAllocated = allocator.getAllocatedMemory();
            memory.deny = true;
            assertThatThrownBy(() -> edge.executeStream(List.of(input))).hasMessageContaining("denied");
            memory.deny = false;
            assertThat(memory.available()).isEqualTo(nativeAvailable);
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowAllocated);
            assertThatThrownBy(() -> edge.executeStream(List.of(input, input))).hasMessageContaining("arity");
            drain(edge.executeStream(List.of(input)));
            // A mismatched output type fails during Java import. Closing the output cancels
            // all native exits before the exception escapes, so the context requires recovery.
            var wrong = new ArrowNativeRegionBridge(context, List.of(TEXT, TEXT), allocator);
            assertThatThrownBy(() -> drain(wrong.executeStream(List.of(input)))).hasMessageContaining("envelope");
            assertThatThrownBy(() -> edge.executeStream(List.of(input))).hasMessageContaining("failed");
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static void drain(ArrowNativeRegionOutput stream) {
        try (stream) {
            ArrowNativeRegionOutput.Batch next;
            while ((next = stream.next()) != null) next.close();
        }
    }

    private static final class DenyingMemory implements NativeMemoryManager {
        private final NativeMemoryManager delegate = TestingNativeMemoryManager.create();
        private boolean deny;

        public boolean tryReserve(long bytes) {
            return !deny && delegate.tryReserve(bytes);
        }

        public void release(long bytes) {
            delegate.release(bytes);
        }

        public long available() {
            return delegate.available();
        }

        public long limit() {
            return delegate.limit();
        }
    }
}
