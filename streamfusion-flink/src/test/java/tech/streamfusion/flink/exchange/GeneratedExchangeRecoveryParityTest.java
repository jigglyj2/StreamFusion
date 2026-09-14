/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowExchangeInputCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExchangeRouter;

class GeneratedExchangeRecoveryParityTest {
    private static final RowType TYPE = RowType.of(
            new IntType(false),
            new IntType(false),
            new VarCharType(),
            new ArrayType(new IntType()),
            new DecimalType(20, 3));
    private static final RowDataSerializer ROWS = new RowDataSerializer(TYPE);

    @TempDir
    Path temporaryDirectory;

    @ParameterizedTest
    @CsvSource({"0,1,17,11", "1,3,127,113", "2,7,4096,1024"})
    void fragmentedRecoveryMatchesFlinkChangelogOnEveryRescaledSubtask(
            int seed, int parallelism, int sourceBufferSize, int outputBufferSize) throws Exception {
        verify(seed, parallelism, sourceBufferSize, outputBufferSize, false);
    }

    @Test
    void spanningArrowFrameSurvivesFlinksDiskSpillingDeserializer() throws Exception {
        verify(3, 1, 32 << 10, 64 << 10, true);
        try (var files = java.nio.file.Files.list(temporaryDirectory)) {
            assertThat(files).isEmpty();
        }
    }

    private void verify(int seed, int parallelism, int sourceBufferSize, int outputBufferSize, boolean large)
            throws Exception {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        int count = large ? 3 : 72;
        var kinds = new RowKind[count];
        var hasTimestamps = new boolean[count];
        var timestamps = new long[count];
        var flinkEvents = new ArrayList<StreamElement>();
        flinkEvents.add(new Watermark(-1));
        for (int index = 0; index < count; index++) {
            var row = GenericRowData.of(
                    index % 13,
                    index,
                    large && index == 1
                            ? StringData.fromString("é".repeat(3 << 20))
                            : index % 5 == 0 ? null : StringData.fromString("é-" + random.nextLong()),
                    index % 7 == 0 ? null : new GenericArrayData(new Integer[] {index, null, -index}),
                    index % 3 == 0
                            ? null
                            : DecimalData.fromBigDecimal(BigDecimal.valueOf(random.nextLong(), 3), 20, 3));
            kinds[index] = RowKind.values()[index % 4];
            row.setRowKind(kinds[index]);
            rows.add(row);
            hasTimestamps[index] = index % 2 != 0;
            timestamps[index] = 100 + index;
            // Flink's generated key selector consumes its normal binary row representation.
            var event = new StreamRecord<RowData>(ROWS.toBinaryRow(row).copy());
            if (hasTimestamps[index]) event.setTimestamp(timestamps[index]);
            flinkEvents.add(event);
        }
        flinkEvents.add(WatermarkStatus.IDLE);
        flinkEvents.add(WatermarkStatus.ACTIVE);
        flinkEvents.add(new Watermark(1000));
        byte[] plan = NativeExchangePlanSerializer.hash(TYPE, new int[] {0}, 128, 1, true);
        var memory = TestingNativeMemoryManager.create();
        try (var allocator = new RootAllocator();
                var batch = ArrowRowDataBatch.transpose(rows, TYPE, allocator)
                        .withEnvelope(kinds, hasTimestamps, timestamps);
                var envelope = ArrowExchangeBatch.withEnvelope(batch, TYPE);
                var router = new NativeExchangeRouter(plan, memory)) {
            var nativeEvents = new ArrayList<StreamElement>();
            nativeEvents.add(new Watermark(-1));
            for (var frame : ArrowExchangeCDataBridge.route(router, envelope.batch()))
                nativeEvents.add(new StreamRecord<>(frame));
            nativeEvents.add(WatermarkStatus.IDLE);
            nativeEvents.add(WatermarkStatus.ACTIVE);
            nativeEvents.add(new Watermark(1000));
            byte[][] inputs = {
                RecoveryFilterFixture.encode(NativeExchangeFrameSerializer.INSTANCE, nativeEvents),
                RecoveryFilterFixture.encode(ROWS, flinkEvents)
            };
            for (int subtask = 0; subtask < parallelism; subtask++) {
                var selector = KeySelectorUtil.getRowDataSelector(
                        getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(TYPE));
                byte[][] filtered = RecoveryFilterFixture.filter(
                        inputs,
                        new TypeSerializer<?>[] {NativeExchangeFrameSerializer.INSTANCE, ROWS},
                        new StreamPartitioner<?>[] {
                            new NativeExchangePartitioner(128), new KeyGroupStreamPartitioner<>(selector, 128)
                        },
                        subtask,
                        parallelism,
                        sourceBufferSize,
                        outputBufferSize,
                        large,
                        temporaryDirectory);
                var expectedFrames = new ArrayList<StreamElement>();
                for (StreamElement event : nativeEvents) {
                    if (!event.isRecord()
                            || ((NativeExchangeFrame) event.asRecord().getValue()).keyGroup() * parallelism / 128
                                    == subtask) expectedFrames.add(event);
                }
                assertThat(filtered[0])
                        .as("serialized IPC and control bytes, seed %s subtask %s", seed, subtask)
                        .containsExactly(
                                RecoveryFilterFixture.encode(NativeExchangeFrameSerializer.INSTANCE, expectedFrames));
                var actual = new ArrayList<StreamElement>();
                for (StreamElement event :
                        RecoveryFilterFixture.decode(NativeExchangeFrameSerializer.INSTANCE, filtered[0])) {
                    if (!event.isRecord()) {
                        actual.add(event);
                        continue;
                    }
                    try (var decoded = ArrowExchangeInputCDataBridge.decode(
                            plan, event.<NativeExchangeFrame>asRecord().getValue(), TYPE, allocator, memory)) {
                        for (int index = 0; index < decoded.size(); index++) {
                            var row = new StreamRecord<RowData>(ROWS.copy(decoded.rowView(index)));
                            if (decoded.hasTimestamp(index)) row.setTimestamp(decoded.timestamp(index));
                            actual.add(row);
                        }
                    }
                }
                var oracle = RecoveryFilterFixture.decode(ROWS, filtered[1]);
                assertThat(RecoveryFilterFixture.encode(ROWS, actual))
                        .as("complete channel changelog and control bytes, seed %s subtask %s", seed, subtask)
                        .containsExactly(RecoveryFilterFixture.encode(ROWS, oracle));
            }
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }
}
