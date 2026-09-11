/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.arrow.NativeRegionArrowFixtures.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.nativebridge.NativeExchangeOutputs;
import tech.streamfusion.nativebridge.NativeExecutionContext;

class GeneratedNativeFrameOutputParityTest {
    @Test
    void directNativeFramesAndMixedArrowConsumersKeepFlinkBytesAndPerKeyOrder() throws Exception {
        for (int seed : List.of(3, 19, 71))
            for (boolean arrow : new boolean[] {false, true}) {
                var memory = TestingNativeMemoryManager.create();
                byte[] aligned = NativeExchangePlanSerializer.hash(TYPE, new int[] {0}, 128, 4, false);
                byte[] unaligned = NativeExchangePlanSerializer.hash(TYPE, new int[] {0}, 128, 4, true);
                List<byte[]> plans = List.of(aligned, unaligned);
                try (var allocator = new RootAllocator(64L << 20);
                        var source = input(allocator, seed);
                        var empty = ArrowRowDataBatch.empty(TYPE, allocator);
                        var context = new NativeExecutionContext(treePlan(), memory)) {
                    NativeExchangeOutputs.bind(context, new int[] {0, 0}, plans, new boolean[] {arrow}, memory);
                    var edge = new ArrowNativeRegionBridge(context, List.of(TYPE), allocator);
                    for (var input : List.of(source, empty, source)) {
                        var expected = new HashMap<Integer, List<org.apache.flink.table.data.binary.BinaryRowData>>();
                        var serializer = new RowDataSerializer(TYPE);
                        for (int row = 0; row < input.size(); row++)
                            expected.computeIfAbsent(
                                            input.rowView(row).isNullAt(0)
                                                    ? null
                                                    : input.rowView(row).getInt(0),
                                            ignored -> new ArrayList<>())
                                    .add(serializer
                                            .toBinaryRow(input.rowView(row))
                                            .copy());
                        var actual = List.of(
                                new HashMap<Integer, List<org.apache.flink.table.data.binary.BinaryRowData>>(),
                                new HashMap<Integer, List<org.apache.flink.table.data.binary.BinaryRowData>>());
                        int arrowRows = 0;
                        try (var stream = edge.executeStream(List.of(input))) {
                            ArrowNativeRegionOutput.Batch next;
                            while ((next = stream.next()) != null)
                                try (var output = next) {
                                    if (!output.isFramed()) {
                                        assertThat(arrow).isTrue();
                                        arrowRows += output.batch().size();
                                        continue;
                                    }
                                    assertThat(output.batch()).isNull();
                                    for (var frame : output.frames()) {
                                        try (var decoded = ArrowExchangeInputCDataBridge.decode(
                                                plans.get(output.port()), frame, TYPE, allocator, memory)) {
                                            var batch = decoded.arrowBatch();
                                            for (int row = 0; row < batch.size(); row++) {
                                                var value = batch.rowView(row);
                                                Integer key = value.isNullAt(0) ? null : value.getInt(0);
                                                actual.get(output.port())
                                                        .computeIfAbsent(key, ignored -> new ArrayList<>())
                                                        .add(serializer
                                                                .toBinaryRow(value)
                                                                .copy());
                                                int sourceRow =
                                                        value.getArray(2).getInt(0);
                                                if (sourceRow >= 0) {
                                                    assertThat(batch.hasTimestamp(row))
                                                            .isEqualTo(input.hasTimestamp(sourceRow));
                                                    if (batch.hasTimestamp(row))
                                                        assertThat(batch.timestamp(row))
                                                                .isEqualTo(input.timestamp(sourceRow));
                                                }
                                            }
                                        }
                                    }
                                }
                        }
                        assertThat(arrowRows).isEqualTo(arrow ? input.size() : 0);
                        assertThat(actual.get(0)).isEqualTo(expected);
                        assertThat(actual.get(1)).isEqualTo(expected);
                    }
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
            }
    }
}
