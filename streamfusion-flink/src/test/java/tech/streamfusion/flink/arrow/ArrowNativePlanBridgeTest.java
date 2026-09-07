/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

class ArrowNativePlanBridgeTest {
    private static final RowType TYPE = RowType.of(new IntType(false), new VarCharType(), new ArrayType(new IntType()));

    @Test
    void streamsGeneratedMultiInputChangelogsWithoutConcatenationAndReusesNegotiatedSchemas() {
        for (int seed = 0; seed < 3; seed++) {
            var memory = TestingNativeMemoryManager.create();
            long available = memory.available();
            try (var allocator = new RootAllocator(64L << 20);
                    var context = new NativeExecutionContext(plan(4), memory);
                    var first = input(allocator, seed, 19);
                    var empty = input(allocator, seed, 0);
                    var second = input(allocator, seed + 100, 37)) {
                var inputs = List.of(empty, first, empty, second);
                var edge = new ArrowNativePlanBridge(context, TYPE, allocator);
                var serializer = new RowDataSerializer(TYPE);
                for (int invocation = 0; invocation < 2; invocation++) {
                    int rows = 0;
                    int pulls = 0;
                    try (var stream = edge.executeStream(inputs)) {
                        NativeCalcResult result;
                        while ((result = stream.nextWithSelection()) != null) {
                            try (var owned = result) {
                                assertThat(owned.batch().size()).isLessThanOrEqualTo(37);
                                ArrowRowDataBatch output = owned.selectEnvelopeFrom(inputs);
                                for (int row = 0; row < output.size(); row++) {
                                    int ordinal = owned.inputRow(row);
                                    ArrowRowDataBatch source = ordinal < first.size() ? first : second;
                                    int local = ordinal < first.size() ? ordinal : ordinal - first.size();
                                    assertThat(serializer
                                                    .toBinaryRow(output.rowView(row))
                                                    .copy())
                                            .isEqualTo(serializer.toBinaryRow(source.rowView(local)));
                                    assertThat(output.rowKind(row)).isEqualTo(source.rowKind(local));
                                    assertThat(output.hasTimestamp(row)).isEqualTo(source.hasTimestamp(local));
                                    if (output.hasTimestamp(row)) {
                                        assertThat(output.timestamp(row)).isEqualTo(source.timestamp(local));
                                    }
                                    rows++;
                                }
                                pulls++;
                            }
                        }
                    }
                    assertThat(rows).isEqualTo(56);
                    assertThat(pulls).isGreaterThanOrEqualTo(2);
                    long count = 56L * (invocation + 1);
                    assertThat(context.metricSnapshot())
                            .containsExactly(
                                    1,
                                    count,
                                    count,
                                    2,
                                    0,
                                    0,
                                    3,
                                    0,
                                    19L * (invocation + 1),
                                    4,
                                    0,
                                    0,
                                    5,
                                    0,
                                    37L * (invocation + 1));
                }
            }
            assertThat(memory.available()).isEqualTo(available);
        }
    }

    @Test
    void earlyDenialAndPartialImportReleaseUnconsumedExportsAndAllowRetry() {
        var memory = new DenyingMemory();
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan(3), memory);
                var input = input(allocator, 0, 4);
                var wrong = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(99)), RowType.of(new IntType(false)), allocator)) {
            var inputs = List.of(input, input, input);
            var edge = new ArrowNativePlanBridge(context, TYPE, allocator);
            drain(edge.executeStream(inputs));
            long nativeAvailable = memory.available();
            long arrowAllocated = allocator.getAllocatedMemory();
            memory.deny = true;
            assertThatThrownBy(() -> edge.executeStream(inputs)).hasMessageContaining("denied");
            memory.deny = false;
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowAllocated);
            assertThat(memory.available()).isEqualTo(nativeAvailable);
            // A new edge has not negotiated Java schemas, but Rust has: fail on the second
            // import, leaving the third exported ArrowArray for Java to release.
            var freshEdge = new ArrowNativePlanBridge(context, TYPE, allocator);
            assertThatThrownBy(() -> freshEdge.executeStream(List.of(input, wrong, input)))
                    .hasMessageContaining("schema changed");
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowAllocated);
            assertThat(memory.available()).isEqualTo(nativeAvailable);
            drain(edge.executeStream(inputs));
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void outputRemainsOwnedAfterContextClosesAndClosedStreamsRejectPolling() {
        var memory = TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan(2), memory);
                var input = input(allocator, 0, 4)) {
            var edge = new ArrowNativePlanBridge(context, TYPE, allocator);
            var stream = edge.executeStream(List.of(input, input));
            try (var output = stream.nextWithSelection()) {
                context.close();
                stream.close();
                stream.close();
                assertThat(output.batch().rowView(0).getInt(0)).isZero();
                assertThatThrownBy(stream::nextWithSelection).hasMessageContaining("closed");
                assertThat(memory.available()).isLessThan(memory.limit());
            }
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static void drain(ArrowCDataBridge.NativeOutputStream stream) {
        try (stream) {
            NativeCalcResult result;
            while ((result = stream.nextWithSelection()) != null) {
                result.close();
            }
        }
    }

    private static ArrowRowDataBatch input(RootAllocator allocator, int seed, int count) {
        List<RowData> rows = new ArrayList<>();
        RowKind[] kinds = new RowKind[count];
        boolean[] present = new boolean[count];
        long[] times = new long[count];
        for (int index = 0; index < count; index++) {
            rows.add(GenericRowData.of(
                    seed + index,
                    index % 3 == 0 ? null : StringData.fromString("é-" + seed + "-" + index),
                    new GenericArrayData(new Integer[] {index, null, seed})));
            kinds[index] = RowKind.values()[index % 4];
            present[index] = index % 2 == 0;
            times[index] = seed * 1000L + index;
        }
        return ArrowRowDataBatch.transpose(rows, TYPE, allocator).withEnvelope(kinds, present, times);
    }

    private static byte[] plan(int count) {
        var union = Union.newBuilder();
        for (int index = 0; index < count; index++) {
            union.addInputs(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(index)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setUnion(union))
                .build()
                .toByteArray();
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
