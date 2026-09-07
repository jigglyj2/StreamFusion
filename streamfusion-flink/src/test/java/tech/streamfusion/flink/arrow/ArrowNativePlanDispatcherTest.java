/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
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
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

class ArrowNativePlanDispatcherTest {
    private static final RowType TYPE = RowType.of(new IntType(), new VarCharType(), new ArrayType(new IntType()));

    @Test
    void generatedArrivalsPreservePayloadEnvelopesAndPerPortCountsWithNoBufferedInputs() {
        for (int seed = 0; seed < 3; seed++) {
            var memory = TestingNativeMemoryManager.create();
            try (var allocator = new RootAllocator(64L << 20);
                    var context = new NativeExecutionContext(plan(3), memory);
                    var dispatcher =
                            new ArrowNativePlanDispatcher(context, List.of(TYPE, TYPE, TYPE), TYPE, allocator)) {
                var random = new Random(seed);
                var serializer = new RowDataSerializer(TYPE);
                long[] counts = new long[3];
                long portBytes = allocator.getAllocatedMemory();
                for (int arrival = 0; arrival < 12; arrival++) {
                    int port = random.nextInt(3);
                    int count = arrival % 4 == 0 ? 0 : 19;
                    var rows = new ArrayList<GenericRowData>();
                    var kinds = new RowKind[count];
                    var presence = new boolean[count];
                    var timestamps = new long[count];
                    for (int index = 0; index < count; index++) {
                        rows.add(GenericRowData.of(
                                index % 3 == 0 ? null : index,
                                index % 5 == 0 ? null : StringData.fromString("port-" + port + "-é-" + arrival),
                                new GenericArrayData(new Integer[] {index, null, arrival})));
                        kinds[index] = RowKind.values()[index % 4];
                        presence[index] = index % 2 == 0;
                        timestamps[index] = arrival * 100L + index;
                    }
                    try (var input = ArrowRowDataBatch.transpose(rows, TYPE, allocator)
                            .withEnvelope(kinds, presence, timestamps)) {
                        int[] seen = {0};
                        dispatcher.process(port, input, output -> {
                            assertThat(output.size()).isPositive().isLessThanOrEqualTo(count);
                            for (int index = 0; index < output.size(); index++) {
                                int original = seen[0]++;
                                assertThat(serializer
                                                .toBinaryRow(output.rowView(index))
                                                .copy())
                                        .isEqualTo(serializer.toBinaryRow(rows.get(original)));
                                assertThat(output.rowKind(index)).isEqualTo(kinds[original]);
                                assertThat(output.hasTimestamp(index)).isEqualTo(presence[original]);
                                if (presence[original]) {
                                    assertThat(output.timestamp(index)).isEqualTo(timestamps[original]);
                                }
                            }
                        });
                        assertThat(seen[0]).isEqualTo(count);
                    }
                    counts[port] += count;
                    assertThat(allocator.getAllocatedMemory()).isEqualTo(portBytes);
                    long total = counts[0] + counts[1] + counts[2];
                    assertThat(context.metricSnapshot())
                            .containsExactly(1, total, total, 2, 0, counts[0], 3, 0, counts[1], 4, 0, counts[2]);
                }
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    @Test
    void callbackFailureAndInvalidOrReentrantCallsReleaseBorrowedInputsAndAllowStatelessRetry() {
        var memory = TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan(2), memory);
                var input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(1, StringData.fromString("x"), new GenericArrayData(new int[] {1}))),
                        TYPE,
                        allocator);
                var wrong = ArrowRowDataBatch.empty(RowType.of(new IntType()), allocator);
                var dispatcher = new ArrowNativePlanDispatcher(context, List.of(TYPE, TYPE), TYPE, allocator)) {
            assertThatThrownBy(() -> dispatcher.process(-1, input, ignored -> {}))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> dispatcher.process(0, wrong, ignored -> {}))
                    .hasMessageContaining("type changed");
            dispatcher.process(0, input, ignored -> {});
            long arrowBytes = allocator.getAllocatedMemory();
            long nativeBytes = memory.available();
            assertThatThrownBy(() -> dispatcher.process(1, input, ignored -> {
                        assertThatThrownBy(() -> dispatcher.process(0, input, nested -> {}))
                                .hasMessageContaining("already processing");
                        assertThatThrownBy(dispatcher::close).hasMessageContaining("during an arrival");
                        throw new IllegalStateException("sink failed");
                    }))
                    .hasMessage("sink failed");
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrowBytes);
            assertThat(memory.available()).isEqualTo(nativeBytes);
            dispatcher.process(
                    0, input, batch -> assertThat(batch.rowView(0).getInt(0)).isEqualTo(1));
            dispatcher.close();
            dispatcher.close();
            assertThatThrownBy(() -> dispatcher.process(0, input, ignored -> {}))
                    .hasMessageContaining("closed");
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static byte[] plan(int count) {
        var union = Union.newBuilder();
        for (int index = 0; index < count; index++) {
            union.addInputs(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(index)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder().setUnion(union))
                .build()
                .toByteArray();
    }
}
