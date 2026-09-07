/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
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
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

class ArrowNativePlanIpcTest {
    @Test
    void generatedFramesMatchDecodedArrowInputsAcrossPortsAndRetainOwnedEnvelopes() throws Exception {
        var type = RowType.of(new IntType(), new VarCharType(), new ArrayType(new IntType()));
        var exchange = NativeExchangePlanSerializer.hash(type, new int[] {0}, 128);
        var union = Union.newBuilder();
        for (int port = 0; port < 2; port++)
            union.addInputs(Operator.newBuilder().setInput(Input.newBuilder().setInputIndex(port)));
        var plan = NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder().setUnion(union))
                .build()
                .toByteArray();
        var memory = TestingNativeMemoryManager.create();
        var serializer = new RowDataSerializer(type);
        try (var allocator = new RootAllocator(64L << 20);
                var nativeContext = new NativeExecutionContext(plan, memory);
                var referenceContext = new NativeExecutionContext(plan, memory);
                var direct = new ArrowNativePlanDispatcher(nativeContext, List.of(type, type), type, allocator);
                var reference = new ArrowNativePlanDispatcher(referenceContext, List.of(type, type), type, allocator)) {
            for (int arrival = 0; arrival < 6; arrival++) {
                var rows = new ArrayList<GenericRowData>();
                var kinds = new RowKind[17];
                var present = new boolean[17];
                var timestamps = new long[17];
                for (int i = 0; i < 17; i++) {
                    rows.add(GenericRowData.of(
                            i % 3,
                            i % 5 == 0 ? null : StringData.fromString("é-" + arrival + "-" + i),
                            new GenericArrayData(new Integer[] {i, null, arrival})));
                    kinds[i] = RowKind.values()[i % 4];
                    present[i] = i % 2 == 0;
                    timestamps[i] = arrival * 100L + i;
                }
                try (var batch = ArrowRowDataBatch.transpose(rows, type, allocator)
                                .withEnvelope(kinds, present, timestamps);
                        var envelope = ArrowExchangeBatch.withEnvelope(batch, type)) {
                    for (var frame : ArrowExchangeCDataBridge.route(exchange, envelope.batch(), allocator, memory)) {
                        var expected = new ArrayList<String>();
                        var actual = new ArrayList<String>();
                        int port = arrival % 2;
                        try (var decoded =
                                ArrowExchangeInputCDataBridge.decode(exchange, frame, type, allocator, memory)) {
                            reference.process(
                                    port, decoded.arrowBatch(), output -> capture(output, serializer, expected));
                            long[] count = {0};
                            direct.processFrame(
                                    port,
                                    exchange,
                                    frame,
                                    n -> count[0] += n,
                                    output -> capture(output, serializer, actual));
                            assertThat(count[0]).isEqualTo(decoded.size());
                        }
                        assertThat(actual).containsExactlyElementsOf(expected);
                    }
                }
                assertThat(nativeContext.metricSnapshot()).containsExactly(referenceContext.metricSnapshot());
            }
            assertThatThrownBy(() -> direct.processFrame(2, exchange, null, ignored -> {}, ignored -> {}))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static void capture(ArrowRowDataBatch batch, RowDataSerializer serializer, List<String> output) {
        for (int i = 0; i < batch.size(); i++) {
            var binary = serializer.toBinaryRow(batch.rowView(i));
            output.add(java.util.Base64.getEncoder()
                            .encodeToString(org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
                                    binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()))
                    + ":" + batch.rowKind(i) + ":" + batch.hasTimestamp(i) + ":"
                    + (batch.hasTimestamp(i) ? batch.timestamp(i) : 0));
        }
    }
}
