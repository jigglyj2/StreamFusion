/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.AttachedSlicingWindowFixture.*;
import static tech.streamfusion.flink.planner.SharedSlicingWindowFixture.resources;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.*;

/** SQL-generated Flink attached MAX/COUNT versus the shared native Arrow tree. */
class AttachedGlobalWindowParityTest {
    @Test
    void generatedChangelogsMatchAtEveryControlOnBothBackends(@TempDir Path directory) throws Exception {
        for (boolean grouping : List.of(false, true))
            for (boolean rocks : List.of(false, true))
                for (int seed = 0; seed < 3; seed++)
                    for (int batchSize : List.of(7, 31))
                        try (var run = new Comparison(
                                grouping,
                                rocks,
                                directory.resolve(grouping + "-" + rocks + "-" + seed + "-" + batchSize),
                                null,
                                null,
                                null)) {
                            var random = new Random(seed);
                            for (int phase = 0; phase < 12; phase++) {
                                var rows = new ArrayList<RowData>();
                                for (int row = 0; row < 31; row++)
                                    rows.add(GenericRowData.of(
                                            row % 11 == 0 ? null : (long) random.nextInt(7),
                                            row % 7 == 0
                                                    ? Long.MIN_VALUE
                                                    : row % 7 == 1 ? Long.MAX_VALUE : random.nextLong(),
                                            (long) random.nextInt(5) + 1,
                                            phase * 2000L + (random.nextInt(9) - 4) * 2000L));
                                for (int offset = 0; offset < rows.size(); offset += batchSize)
                                    run.process(rows.subList(offset, Math.min(rows.size(), offset + batchSize)));
                                if (phase % 3 == 2) run.preBarrier(phase);
                                run.watermark(phase == 11 ? Long.MAX_VALUE : phase * 2000L - 1);
                            }
                        }
    }

    @Test
    void restoredFlinkClockPrecedesReplayedInputAndWatermarks(@TempDir Path directory) throws Exception {
        for (boolean grouping : List.of(false, true))
            for (boolean rocks : List.of(false, true)) {
                OperatorSubtaskState flinkSnapshot;
                var nativeSnapshots = new ArrayList<byte[]>();
                try (var run = new Comparison(
                        grouping, rocks, directory.resolve("source-" + grouping + "-" + rocks), null, null, null)) {
                    run.process(List.of(GenericRowData.of(1L, 2L, 1L, 2000L), GenericRowData.of(1L, 9L, 2L, 4000L)));
                    run.watermark(1999);
                    run.process(List.of(GenericRowData.of(2L, 3L, 1L, 4000L)));
                    run.preBarrier(7);
                    flinkSnapshot = run.flink.snapshot(7, 0);
                    for (int group = 0; group < 128; group++)
                        nativeSnapshots.add(run.context.state().snapshot(3, group));
                }
                try (var run = new Comparison(
                        grouping,
                        rocks,
                        directory.resolve("restore-" + grouping + "-" + rocks),
                        flinkSnapshot,
                        nativeSnapshots,
                        1999L)) {
                    run.process(List.of(GenericRowData.of(7L, 13L, 1L, 2000L), GenericRowData.of(2L, 11L, 2L, 4000L)));
                    run.watermark(1999);
                    run.watermark(3999);
                    run.watermark(5999);
                    run.watermark(7999);
                    run.process(List.of(GenericRowData.of(3L, 11L, 1L, 2000L)));
                    run.watermark(Long.MAX_VALUE);
                } finally {
                    flinkSnapshot.discardState();
                }
            }
    }

    private static final class Comparison implements AutoCloseable {
        final SharedAggregateRegionParityTest.Memory memory = new SharedAggregateRegionParityTest.Memory();
        final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink;
        final RootAllocator allocator;
        final NativeExecutionContext context;
        final ArrowNativePlanDispatcher dispatcher;
        final boolean grouping;
        final org.apache.flink.table.types.logical.RowType inputType;
        final RowDataSerializer inputSerializer;
        final RowDataSerializer outputSerializer;
        long inputs, outputs;

        Comparison(
                boolean grouping,
                boolean rocks,
                Path directory,
                OperatorSubtaskState snapshot,
                List<byte[]> nativeSnapshots,
                Long watermark)
                throws Exception {
            this.grouping = grouping;
            inputType = type(INPUT, grouping);
            inputSerializer = new RowDataSerializer(type(FLINK_INPUT, grouping));
            outputSerializer = new RowDataSerializer(type(OUTPUT, grouping));
            var stage = SlicingWindowFlinkPlan.stage("GlobalWindowAggregate", sql(grouping));
            assertThat(stage.getDescription())
                    .contains("win_end=")
                    .contains("MAX(")
                    .contains("COUNT(");
            assertThat(((org.apache.flink.table.runtime.typeutils.InternalTypeInfo<?>) stage.getInputType())
                            .toRowType()
                            .getChildren())
                    .isEqualTo(type(FLINK_INPUT, grouping).getChildren());
            assertThat(((org.apache.flink.table.runtime.typeutils.InternalTypeInfo<?>) stage.getOutputType())
                            .toRowType()
                            .getChildren())
                    .isEqualTo(type(OUTPUT, grouping).getChildren());
            flink = GlobalWindowFlinkOracle.create(stage, rocks, snapshot);
            allocator = new RootAllocator(64L << 20);
            context = new NativeExecutionContext(plan(grouping), memory, resources(rocks, directory, watermark));
            if (nativeSnapshots != null)
                for (int group = 0; group < nativeSnapshots.size(); group++)
                    context.state().restore(3, group, nativeSnapshots.get(group));
            dispatcher = new ArrowNativePlanDispatcher(context, List.of(inputType), type(OUTPUT, grouping), allocator);
            assertThat(context.hasStateBindings()).isTrue();
            assertThat(context.requiresInputEnvelope()).isTrue();
        }

        void process(List<RowData> rows) throws Exception {
            var nativeRows = new ArrayList<RowData>();
            for (var row : rows) {
                var flinkRow = grouping ? row : GenericRowData.of(row.getLong(1), row.getLong(2), row.getLong(3));
                flink.processElement(new StreamRecord<>(inputSerializer.toBinaryRow(flinkRow), 123));
                nativeRows.add(
                        grouping
                                ? GenericRowData.of(
                                        row.isNullAt(0) ? null : row.getLong(0),
                                        partial(row.getLong(1), row.getLong(2)),
                                        row.getLong(3) - 6000,
                                        row.getLong(3))
                                : GenericRowData.of(
                                        partial(row.getLong(1), row.getLong(2)),
                                        row.getLong(3) - 6000,
                                        row.getLong(3)));
            }
            inputs += rows.size();
            var actual = new ArrayList<byte[]>();
            try (var batch = ArrowRowDataBatch.transpose(nativeRows, inputType, allocator)) {
                dispatcher.process(0, batch, output -> append(output, actual));
            }
            compare(actual);
        }

        void watermark(long value) throws Exception {
            flink.processWatermark(new Watermark(value));
            control(NativeStageControl.newBuilder().setPlanNodeId(3).setWatermarkMillis(value));
        }

        void preBarrier(long value) throws Exception {
            flink.prepareSnapshotPreBarrier(value);
            control(NativeStageControl.newBuilder().setPlanNodeId(3).setBeforeCheckpoint(value));
        }

        void control(NativeStageControl.Builder stage) throws Exception {
            var actual = new ArrayList<byte[]>();
            dispatcher.control(
                    NativeControlInvocation.newBuilder()
                            .setProtocolVersion(1)
                            .addStages(stage)
                            .build()
                            .toByteArray(),
                    output -> append(output, actual));
            compare(actual);
        }

        void append(ArrowRowDataBatch batch, List<byte[]> rows) {
            for (int row = 0; row < batch.size(); row++) {
                assertThat(batch.hasTimestamp(row)).isFalse();
                assertThat(batch.rowKind(row)).isEqualTo(RowKind.INSERT);
                rows.add(bytes(batch.rowView(row)));
            }
        }

        byte[] bytes(RowData row) {
            try {
                var bytes = new DataOutputSerializer(64);
                outputSerializer.serialize(row, bytes);
                return bytes.getCopyOfBuffer();
            } catch (java.io.IOException failure) {
                throw new RuntimeException(failure);
            }
        }

        void compare(List<byte[]> actual) throws Exception {
            var expected = new ArrayList<byte[]>();
            for (var event : flink.getOutput())
                if (event instanceof StreamRecord<?>) {
                    var record = (StreamRecord<?>) event;
                    assertThat(record.hasTimestamp()).isFalse();
                    var row = (RowData) record.getValue();
                    assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
                    expected.add(bytes(row));
                }
            flink.getOutput().clear();
            outputs += expected.size();
            // Timer ties between independent keys are not ordered by Flink. Keep control
            // boundaries exact and compare the full serialized changelog records within each.
            actual.sort(Arrays::compareUnsigned);
            expected.sort(Arrays::compareUnsigned);
            assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
}
