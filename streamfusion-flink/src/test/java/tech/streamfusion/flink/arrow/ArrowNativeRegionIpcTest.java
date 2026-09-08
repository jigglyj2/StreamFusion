/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.arrow.NativeRegionArrowFixtures.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeRegionStream;
import tech.streamfusion.proto.plan.v1.*;

class ArrowNativeRegionIpcTest {
    @Test
    void generatedIpcInputsMatchCDataAcrossPortsWithOneSharedProducer() throws Exception {
        byte[] plan = twoInputs();
        for (byte[] exchange : List.of(
                NativeExchangePlanSerializer.singleton(TYPE),
                NativeExchangePlanSerializer.hash(TYPE, new int[] {0}, 128, 4, false))) {
            var memory = TestingNativeMemoryManager.create();
            try (var allocator = new RootAllocator(64L << 20);
                    var empty = ArrowRowDataBatch.transpose(List.of(), TYPE, allocator);
                    var context = NativeExecutionContext.region(plan, memory, null, null);
                    var referenceContext = NativeExecutionContext.region(plan, memory, null, null)) {
                var direct = new ArrowNativeRegionBridge(context, List.of(TYPE, TEXT), allocator);
                var reference = new ArrowNativeRegionBridge(referenceContext, List.of(TYPE, TEXT), allocator);
                for (int arrival = 0; arrival < 6; arrival++) {
                    try (var input = input(allocator, 3 + arrival * 17);
                            var envelope = ArrowExchangeBatch.withEnvelope(input, TYPE)) {
                        for (var frame :
                                ArrowExchangeCDataBridge.route(exchange, envelope.batch(), allocator, memory)) {
                            try (var decoded =
                                    ArrowExchangeInputCDataBridge.decode(exchange, frame, TYPE, allocator, memory)) {
                                int port = arrival % 2;
                                var expectedInputs = port == 0
                                        ? List.of(decoded.arrowBatch(), empty)
                                        : List.of(empty, decoded.arrowBatch());
                                var expected = capture(reference.executeStream(expectedInputs));
                                long[] rows = {-1};
                                var actual = capture(direct.executeExchangeStream(
                                        List.of(empty, empty), port, exchange, frame, n -> rows[0] = n));
                                assertThat(rows[0]).isEqualTo(decoded.size());
                                assertThat(actual).containsExactlyElementsOf(expected);
                                assertThat(context.metricSnapshot()).containsExactly(referenceContext.metricSnapshot());
                            }
                        }
                    }
                }
                assertThatThrownBy(
                                () -> direct.executeExchangeStream(List.of(empty, empty), 2, exchange, null, n -> {}))
                        .isInstanceOf(IndexOutOfBoundsException.class);
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    @Test
    void decodeFailureAllowsRetryAndCallbackFailureCancelsTheOwnedInvocation() throws Exception {
        var memory = TestingNativeMemoryManager.create();
        byte[] exchange = NativeExchangePlanSerializer.singleton(TYPE);
        try (var allocator = new RootAllocator(64L << 20);
                var empty = ArrowRowDataBatch.transpose(List.of(), TYPE, allocator);
                var input = input(allocator, 3);
                var envelope = ArrowExchangeBatch.withEnvelope(input, TYPE);
                var context = NativeExecutionContext.region(plan(), memory, null, null)) {
            var direct = new ArrowNativeRegionBridge(context, List.of(TYPE, TEXT), allocator);
            var frame = ArrowExchangeCDataBridge.route(exchange, envelope.batch(), allocator, memory)
                    .get(0);
            long available = memory.available();
            long arrow = allocator.getAllocatedMemory();
            assertThatThrownBy(() -> NativeRegionStream.openExchange(
                            context, 0, exchange, new byte[1], 1, 1, 0, new long[1], new long[1], n -> {}))
                    .hasMessageContaining("range");
            assertThat(memory.available()).isEqualTo(available);
            var invalid = new NativeExchangeFrame(0, new byte[] {0, 1, 2}, new byte[0]);
            assertThatThrownBy(() -> direct.executeExchangeStream(List.of(empty), 0, exchange, invalid, n -> {}))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(memory.available()).isEqualTo(available);
            assertThat(allocator.getAllocatedMemory()).isEqualTo(arrow);
            var values = capture(direct.executeExchangeStream(
                    List.of(empty), 0, exchange, frame, n -> assertThat(n).isEqualTo(37)));
            assertThat(values.get(0)).hasSize(37);
            assertThat(values.get(1)).hasSize(37);
            assertThatThrownBy(() -> direct.executeExchangeStream(List.of(input), 0, exchange, frame, n -> {}))
                    .hasMessageContaining("empty");
            assertThatThrownBy(() -> direct.executeExchangeStream(List.of(empty), 0, exchange, frame, n -> {
                        throw new IllegalStateException("metric callback failed");
                    }))
                    .hasMessageContaining("metric callback failed");
            assertThatThrownBy(() -> direct.executeStream(List.of(empty))).hasMessageContaining("failed");
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static List<List<String>> capture(ArrowNativeRegionOutput stream) {
        var output = List.of(new ArrayList<String>(), new ArrayList<String>());
        long[] sharedStringBuffer = {0};
        try (stream) {
            ArrowNativeRegionOutput.Batch next;
            while ((next = stream.next()) != null) {
                try (var value = next) {
                    var serializer = new RowDataSerializer(value.port() == 0 ? TYPE : TEXT);
                    var batch = value.batch();
                    if (batch.size() > 0) {
                        long address = batch.root()
                                .getVector(value.port() == 0 ? 1 : 0)
                                .getDataBuffer()
                                .memoryAddress();
                        if (sharedStringBuffer[0] == 0) sharedStringBuffer[0] = address;
                        else assertThat(address).isEqualTo(sharedStringBuffer[0]);
                    }
                    for (int row = 0; row < batch.size(); row++) {
                        var binary = serializer.toBinaryRow(batch.rowView(row)).copy();
                        binary.setRowKind(batch.rowKind(row));
                        output.get(value.port())
                                .add(java.util.Base64.getEncoder()
                                                .encodeToString(
                                                        org.apache.flink.table.data.binary.BinarySegmentUtils
                                                                .copyToBytes(
                                                                        binary.getSegments(),
                                                                        binary.getOffset(),
                                                                        binary.getSizeInBytes()))
                                        + ":" + batch.hasTimestamp(row) + ":"
                                        + (batch.hasTimestamp(row) ? batch.timestamp(row) : 0));
                    }
                }
            }
        }
        return List.copyOf(output);
    }

    private static byte[] twoInputs() throws Exception {
        var region = NativeRegionPlan.parseFrom(plan()).toBuilder().setInputCount(2);
        var union = Union.newBuilder();
        for (int port = 0; port < 2; port++)
            union.addInputs(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(port)));
        region.setStages(
                0,
                NativeRegionStage.newBuilder()
                        .setOperator(Operator.newBuilder().setPlanNodeId(11).setUnion(union))
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(0))
                        .addInputs(NativeRegionInputReference.newBuilder().setExternalInput(1)));
        return region.build().toByteArray();
    }
}
