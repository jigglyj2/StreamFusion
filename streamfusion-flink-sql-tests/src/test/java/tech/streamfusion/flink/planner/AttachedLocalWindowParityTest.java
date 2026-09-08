/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.planner.window.StreamFusionLocalWindowAggregateTranslator;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.*;

/** SQL-generated attached MAX/COUNT local slices, with an end-only native assignment contract. */
class AttachedLocalWindowParityTest {
    @Test
    @SuppressWarnings("unchecked")
    void generatedPartialsAndControlFlushesMatchFlinkWithEndOnlyAttachment() throws Exception {
        for (boolean grouped : List.of(false, true)) {
            var stage = SlicingWindowFlinkPlan.stage("LocalWindowAggregate", AttachedSlicingWindowFixture.sql(grouped));
            var input = ((InternalTypeInfo<RowData>) stage.getInputType()).toRowType();
            var flinkOutput = ((InternalTypeInfo<RowData>) stage.getOutputType()).toRowType();
            int end = input.getFieldIndex("window_end");
            int value = input.getFieldIndex("n");
            int key = input.getFieldIndex("k");
            assertThat(end).isNotNegative();
            assertThat(value).isNotNegative();
            int start = input.getFieldIndex("window_start");
            var fields = new ArrayList<LogicalType>();
            if (grouped) fields.add(input.getTypeAt(key));
            fields.add(new VarBinaryType(false, VarBinaryType.MAX_LENGTH));
            fields.add(new BigIntType(false));
            fields.add(new BigIntType(false));
            var partial = RowType.of(fields.toArray(new LogicalType[0]));
            byte[] plan = SharedSlicingWindowFixture.compose(
                    StreamFusionLocalWindowAggregateTranslator.createStagePlan(
                            input,
                            partial,
                            grouped ? new int[] {key} : new int[0],
                            new org.apache.calcite.rel.core.AggregateCall[] {
                                SharedSlicingWindowFixture.call(
                                        org.apache.calcite.sql.fun.SqlStdOperatorTable.MAX,
                                        List.of(value),
                                        input.getTypeAt(value)),
                                SharedSlicingWindowFixture.call(
                                        org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
                                        List.of(),
                                        new BigIntType(false))
                            },
                            new org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy(
                                    SharedSlicingWindowFixture.hop(),
                                    new TimestampType(
                                            false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3),
                                    end),
                            false,
                            SharedSlicingWindowFixture.config()),
                    input.getFieldCount(),
                    partial.getFieldCount());
            for (int seed = 0; seed < 3; seed++)
                for (int batchSize : List.of(7, 31)) {
                    var memory = new SharedAggregateRegionParityTest.Memory();
                    // Each harness must own a new generated Flink operator.
                    var original = SlicingWindowFlinkPlan.stage(
                            "LocalWindowAggregate", AttachedSlicingWindowFixture.sql(grouped));
                    try (var flink = LocalWindowFlinkOracle.create(original, 3L << 20);
                            var allocator = new RootAllocator(64L << 20);
                            var context = new NativeExecutionContext(plan, memory, null, resources());
                            var dispatcher =
                                    new ArrowNativePlanDispatcher(context, List.of(input), partial, allocator)) {
                        var random = new Random(seed);
                        var serializer = new RowDataSerializer(flinkOutput);
                        long inputs = 0, outputs = 0;
                        for (int phase = 0; phase < 12; phase++) {
                            var rows = new ArrayList<RowData>();
                            for (int r = 0; r < 31; r++) {
                                var row = new GenericRowData(input.getFieldCount());
                                if (key >= 0) row.setField(key, r % 11 == 0 ? null : (long) random.nextInt(7));
                                row.setField(
                                        value,
                                        r % 7 == 0 ? Long.MIN_VALUE : r % 7 == 1 ? Long.MAX_VALUE : random.nextLong());
                                row.setField(end, TimestampData.fromEpochMillis((random.nextInt(13) - 6) * 2000L));
                                // The source start is irrelevant to Flink's local assignment, even when retained.
                                if (start >= 0) row.setField(start, TimestampData.fromEpochMillis(random.nextLong()));
                                rows.add(row);
                            }
                            for (int offset = 0; offset < rows.size(); offset += batchSize) {
                                var chunk = rows.subList(offset, Math.min(rows.size(), offset + batchSize));
                                for (var row : chunk) flink.processElement(new StreamRecord<>(row, 123));
                                inputs += chunk.size();
                                var actual = new DataOutputSerializer(128);
                                try (var batch = ArrowRowDataBatch.transpose(chunk, input, allocator)) {
                                    dispatcher.process(0, batch, output -> append(output, grouped, serializer, actual));
                                }
                                outputs += compare(flink.getOutput(), actual, serializer);
                            }
                            var actual = new DataOutputSerializer(128);
                            var control = NativeStageControl.newBuilder().setPlanNodeId(3);
                            if (phase % 3 == 1) {
                                flink.prepareSnapshotPreBarrier(phase);
                                control.setBeforeCheckpoint(phase);
                            } else {
                                long watermark = phase == 11 ? Long.MAX_VALUE : phase * 2000L - 1;
                                flink.processWatermark(new Watermark(watermark));
                                control.setWatermarkMillis(watermark);
                            }
                            dispatcher.control(
                                    NativeControlInvocation.newBuilder()
                                            .setProtocolVersion(1)
                                            .addStages(control)
                                            .build()
                                            .toByteArray(),
                                    output -> append(output, grouped, serializer, actual));
                            outputs += compare(flink.getOutput(), actual, serializer);
                            assertThat(context.metricSnapshot())
                                    .containsExactly(
                                            4, outputs, outputs, 3, inputs, outputs, 2, inputs, inputs, 1, 0, inputs);
                        }
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                }
        }
    }

    private static void append(
            ArrowRowDataBatch batch, boolean grouped, RowDataSerializer serializer, DataOutputSerializer out) {
        try {
            int keys = grouped ? 1 : 0;
            for (int index = 0; index < batch.size(); index++) {
                assertThat(batch.rowKind(index)).isEqualTo(RowKind.INSERT);
                assertThat(batch.hasTimestamp(index)).isFalse();
                var row = batch.rowView(index);
                byte[] bytes = row.getBinary(keys);
                assertThat(bytes).hasSize(45);
                var state = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                long maximum = state.getLong(20), count = state.getLong(37);
                assertThat(state.getLong(5)).isEqualTo(count);
                assertThat(row.getLong(keys + 1)).isEqualTo(row.getLong(keys + 2) - 6000);
                var decoded = new GenericRowData(keys + 3);
                if (grouped) decoded.setField(0, row.isNullAt(0) ? null : row.getLong(0));
                decoded.setField(keys, maximum);
                decoded.setField(keys + 1, count);
                decoded.setField(keys + 2, row.getLong(keys + 2));
                serializer.serialize(decoded, out);
            }
        } catch (java.io.IOException failure) {
            throw new RuntimeException(failure);
        }
    }

    private static long compare(
            java.util.Queue<Object> events, DataOutputSerializer actual, RowDataSerializer serializer)
            throws Exception {
        var expected = new DataOutputSerializer(128);
        long count = 0;
        for (var event : events)
            if (event instanceof StreamRecord<?>) {
                var record = (StreamRecord<?>) event;
                assertThat(record.hasTimestamp()).isFalse();
                serializer.serialize((RowData) record.getValue(), expected);
                count++;
            }
        events.clear();
        assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
        return count;
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
