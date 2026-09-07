/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.deduplicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowNativePlanBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.Deduplicate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** All key encodings through the common protocol-3 tree/edge, with Flink's actual keyed collector. */
class SharedDeduplicateEnvelopeParityTest {
    @TempDir
    Path directory;

    @ParameterizedTest(name = "owned envelope: {0}")
    @MethodSource("tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateKeyTypeParityTest#keyCases")
    void historicalUpdatesSelectTheTriggeringRecordEnvelope(String description, LogicalType keyType, Object key)
            throws Exception {
        var type = RowType.of(keyType, new TimestampType(false, 3), new BigIntType(false));
        var info = InternalTypeInfo.<RowData>of(type);
        var serializer = new RowDataSerializer(StreamFusionDeduplicateKeyTypeParityTest.physicalRowType(type));
        for (boolean rocks : List.of(false, true))
            for (boolean before : List.of(false, true)) {
                var selector = KeySelectorUtil.getRowDataSelector(getClass().getClassLoader(), new int[] {0}, info);
                var memory = TestingNativeMemoryManager.create();
                long initial = memory.available();
                var binding = rocks
                        ? NativeStateResources.rocksDb(2, 16, 0, 15, directory.resolve("before-" + before), 4L << 20)
                        : NativeStateResources.memory(2, 16, 0, 15);
                try (var oracle = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                                new KeyedProcessOperator<>(
                                        new RowTimeDeduplicateFunction(info, 0L, 1, before, true, true)),
                                selector,
                                selector.getProducedType(),
                                16,
                                1,
                                0);
                        var allocator = new RootAllocator(64L << 20);
                        var context = new NativeExecutionContext(
                                plan(before), memory, NativeStateResources.serialize(List.of(binding)))) {
                    oracle.setup(serializer);
                    oracle.open();
                    var bridge = new ArrowNativePlanBridge(context, type, allocator);
                    // Separate arrivals force historical-state UPDATE_BEFORE materialization.
                    for (int arrival = 0; arrival < 4; arrival++) {
                        int count = new int[] {0, 7, 1, 17}[arrival];
                        var rows = new ArrayList<RowData>();
                        var present = new boolean[count];
                        var times = new long[count];
                        for (int row = 0; row < count; row++) {
                            rows.add(GenericRowData.of(
                                    row % 3 == 0 ? null : key,
                                    TimestampData.fromEpochMillis(100L * arrival + row % 4),
                                    (long) row + arrival * 100));
                            present[row] = row % 3 != 0;
                            times[row] = new long[] {Long.MIN_VALUE, 0, -19, Long.MAX_VALUE}[row % 4];
                            RowData oracleRow =
                                    serializer.toBinaryRow(rows.get(row)).copy();
                            oracle.processElement(
                                    present[row]
                                            ? new StreamRecord<>(oracleRow, times[row])
                                            : new StreamRecord<>(oracleRow));
                        }
                        var expected = new DataOutputSerializer(128);
                        for (var event : oracle.getOutput()) {
                            @SuppressWarnings("unchecked")
                            var record = (StreamRecord<RowData>) event;
                            encode(
                                    serializer,
                                    record.getValue(),
                                    record.hasTimestamp(),
                                    record.getTimestamp(),
                                    expected);
                        }
                        oracle.getOutput().clear();
                        var actual = new DataOutputSerializer(128);
                        try (var input = ArrowRowDataBatch.transpose(rows, type, allocator)
                                        .withEnvelope(
                                                rows.stream()
                                                        .map(RowData::getRowKind)
                                                        .toArray(org.apache.flink.types.RowKind[]::new),
                                                present,
                                                times);
                                var stream = bridge.executeStream(List.of(input))) {
                            NativeCalcResult next;
                            while ((next = stream.nextWithSelection()) != null)
                                try (var result = next) {
                                    var output = result.selectEnvelopeFrom(
                                            List.of()); // Detached metadata needs no input owner.
                                    for (int row = 0; row < output.size(); row++) {
                                        var value = output.rowView(row);
                                        value.setRowKind(output.rowKind(row));
                                        encode(
                                                serializer,
                                                value,
                                                output.hasTimestamp(row),
                                                output.timestamp(row),
                                                actual);
                                    }
                                }
                        }
                        assertThat(actual.getCopyOfBuffer())
                                .as("%s rocks=%s before=%s arrival=%s", description, rocks, before, arrival)
                                .containsExactly(expected.getCopyOfBuffer());
                    }
                    assertThat(context.metricSnapshot()[1]).isEqualTo(25);
                }
                assertThat(memory.available()).isEqualTo(initial);
            }
    }

    private static byte[] plan(boolean before) {
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(2)
                        .setDeduplicate(Deduplicate.newBuilder()
                                .setInput(Operator.newBuilder().setPlanNodeId(1).setInput(Input.newBuilder()))
                                .addKeyIndices(0)
                                .setOrderIndex(1)
                                .setKeepLast(true)
                                .setGenerateInsert(true)
                                .setGenerateUpdateBefore(before)))
                .build()
                .toByteArray();
    }

    private static void encode(
            RowDataSerializer serializer, RowData row, boolean present, long timestamp, DataOutputSerializer output)
            throws Exception {
        serializer.serialize(row, output);
        output.writeBoolean(present);
        if (present) output.writeLong(timestamp);
    }
}
