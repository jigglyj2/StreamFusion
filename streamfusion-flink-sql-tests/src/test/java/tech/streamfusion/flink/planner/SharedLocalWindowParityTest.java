/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.streamfusion.flink.planner.SharedLocalWindowFixture.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.*;

/** Real SQL-generated Flink local slicer versus Calc -> buffered native local window -> Calc. */
class SharedLocalWindowParityTest {
    private static final RowType FLINK_PARTIAL =
            RowType.of(new BigIntType(), new BigIntType(false), new BigIntType(false));

    @Test
    void generatedControlChangelogsMatchFlinkAcrossArrowBatchSizes() throws Exception {
        for (int seed = 0; seed < 3; seed++)
            for (int batchSize : List.of(7, 31)) {
                try (var run = new Comparison()) {
                    var random = new Random(seed);
                    for (int phase = 0; phase < 12; phase++) {
                        var rows = new ArrayList<RowData>();
                        for (int row = 0; row < 31; row++)
                            rows.add(GenericRowData.of(
                                    row % 11 == 0 ? null : (long) random.nextInt(17),
                                    TimestampData.fromEpochMillis(random.nextInt(16001) - 8000)));
                        for (int offset = 0; offset < rows.size(); offset += batchSize)
                            run.process(rows.subList(offset, Math.min(rows.size(), offset + batchSize)));
                        if (phase % 3 == 2) run.watermark(phase * 1000L - 3000);
                        if (phase == 6) run.checkpoint(7);
                    }
                    run.watermark(Long.MAX_VALUE);
                    run.checkpoint(8);
                }
            }
    }

    @Test
    void pressureFlushChangelogMatchesFlinkWithResolvedThreeMebibyteCapacity() throws Exception {
        for (int batchSize : List.of(4096, 16384))
            try (var run = new Comparison()) {
                for (int start = 0; start < 180000; start += batchSize) {
                    var rows = new ArrayList<RowData>();
                    for (int row = start; row < Math.min(180000, start + batchSize); row++)
                        rows.add(GenericRowData.of((long) (row % 17), TimestampData.fromEpochMillis(1000)));
                    run.process(rows);
                }
                assertThat(run.outputs).isEqualTo(34);
                run.checkpoint(1);
                assertThat(run.outputs).isEqualTo(51);
            }
    }

    @Test
    void rejectedTaskResourceConstructionReturnsAllJniAndNativeCredit() throws Exception {
        var memory = new SharedAggregateRegionParityTest.Memory();
        var invalid = NativeTaskBindings.parseFrom(resources()).toBuilder()
                .setProtocolVersion(99)
                .build()
                .toByteArray();
        assertThatThrownBy(() -> new NativeExecutionContext(plan(), memory, null, invalid))
                .hasMessageContaining("protocol");
        assertThat(memory.available()).isEqualTo(memory.limit());
        try (var context = new NativeExecutionContext(plan(), memory, null, resources())) {
            assertThat(context.requiresInputEnvelope()).isTrue();
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void nullableRowtimeSchemaFailsActualNullsLikeTheFlinkSlicer() throws Exception {
        var input = RowType.of(new BigIntType(), new org.apache.flink.table.types.logical.TimestampType(true, 3));
        var memory = new SharedAggregateRegionParityTest.Memory();
        var row = GenericRowData.of(1L, null);
        try (var flink = LocalWindowFlinkOracle.create(3L << 20);
                var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan(input), memory, null, resources());
                var dispatcher = new ArrowNativePlanDispatcher(context, List.of(input), PARTIAL, allocator);
                var batch = ArrowRowDataBatch.transpose(List.of(row), input, allocator)) {
            assertThatThrownBy(() -> flink.processElement(new StreamRecord<RowData>(row)))
                    .hasMessageContaining("RowTime field should not be null");
            assertThatThrownBy(() -> dispatcher.process(0, batch, output -> {
                        throw new AssertionError("Null rowtime must fail before emitting partials");
                    }))
                    .rootCause()
                    .hasMessageContaining("RowTime field should not be null");
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static final class Comparison implements AutoCloseable {
        final SharedAggregateRegionParityTest.Memory memory = new SharedAggregateRegionParityTest.Memory();
        final OneInputStreamOperatorTestHarness<RowData, RowData> flink;
        final RootAllocator allocator;
        final NativeExecutionContext context;
        final ArrowNativePlanDispatcher dispatcher;
        final RowDataSerializer serializer = new RowDataSerializer(FLINK_PARTIAL);
        long inputs, outputs;

        Comparison() throws Exception {
            flink = LocalWindowFlinkOracle.create(3L << 20);
            allocator = new RootAllocator(64L << 20);
            context = new NativeExecutionContext(plan(), memory, null, resources());
            dispatcher = new ArrowNativePlanDispatcher(context, List.of(INPUT), PARTIAL, allocator);
            assertThat(context.hasStateBindings()).isFalse();
            assertThat(context.requiresInputEnvelope()).isTrue();
        }

        void process(List<RowData> rows) throws Exception {
            for (var row : rows) flink.processElement(new StreamRecord<>(row, 123));
            inputs += rows.size();
            var actual = new DataOutputSerializer(128);
            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)) {
                dispatcher.process(0, batch, output -> append(output, actual));
            }
            compare(actual);
        }

        void watermark(long value) throws Exception {
            flink.processWatermark(new Watermark(value));
            control(NativeStageControl.newBuilder().setPlanNodeId(3).setWatermarkMillis(value));
        }

        void checkpoint(long id) throws Exception {
            flink.prepareSnapshotPreBarrier(id);
            control(NativeStageControl.newBuilder().setPlanNodeId(3).setBeforeCheckpoint(id));
        }

        void control(NativeStageControl.Builder stage) throws Exception {
            var actual = new DataOutputSerializer(128);
            dispatcher.control(
                    NativeControlInvocation.newBuilder()
                            .setProtocolVersion(1)
                            .addStages(stage)
                            .build()
                            .toByteArray(),
                    output -> append(output, actual));
            compare(actual);
        }

        void append(ArrowRowDataBatch output, DataOutputSerializer target) {
            try {
                for (int row = 0; row < output.size(); row++) {
                    assertThat(output.hasTimestamp(row)).isFalse();
                    assertThat(output.rowKind(row)).isEqualTo(RowKind.INSERT);
                    var value = output.rowView(row);
                    var encoded = value.getBinary(1);
                    assertThat(encoded).hasSize(26);
                    assertThat(java.util.Arrays.copyOf(encoded, 5))
                            .containsExactly((byte) 'S', (byte) 'F', (byte) 'G', (byte) 'A', (byte) 6);
                    var state = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
                    long count = state.getLong(5);
                    assertThat(state.getInt(13)).isEqualTo(1);
                    assertThat(state.get(17)).isEqualTo((byte) 1);
                    assertThat(state.getLong(18)).isEqualTo(count);
                    assertThat(value.getLong(2)).isEqualTo(value.getLong(3) - 2000);
                    serializer.serialize(
                            GenericRowData.of(value.isNullAt(0) ? null : value.getLong(0), count, value.getLong(3)),
                            target);
                }
            } catch (java.io.IOException failure) {
                throw new RuntimeException(failure);
            }
        }

        void compare(DataOutputSerializer actual) throws Exception {
            var expected = new DataOutputSerializer(128);
            for (var event : flink.getOutput())
                if (event instanceof StreamRecord<?>) {
                    var record = (StreamRecord<?>) event;
                    assertThat(record.hasTimestamp()).isFalse();
                    serializer.serialize((RowData) record.getValue(), expected);
                    outputs++;
                }
            flink.getOutput().clear();
            assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
            assertThat(context.metricSnapshot())
                    .containsExactly(4, outputs, outputs, 3, inputs, outputs, 2, inputs, inputs, 1, 0, inputs);
        }

        @Override
        public void close() throws Exception {
            try {
                dispatcher.close();
            } finally {
                try {
                    context.close();
                } finally {
                    try {
                        allocator.close();
                    } finally {
                        flink.close();
                    }
                }
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    private static byte[] resources() {
        return NativeTaskBindings.newBuilder()
                .setProtocolVersion(1)
                .addBindings(NativeTaskBinding.newBuilder()
                        .setPlanNodeId(3)
                        .setLocalWindowBuffer(NativeLocalWindowBuffer.newBuilder()
                                .setFlinkBufferMemoryBytes(3L << 20)
                                .setFlinkPageBytes(32 << 10)))
                .build()
                .toByteArray();
    }
}
