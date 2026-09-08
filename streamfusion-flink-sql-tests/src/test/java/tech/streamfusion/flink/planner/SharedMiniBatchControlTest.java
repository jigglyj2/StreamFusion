/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativeControlEndInput;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeStageControl;

class SharedMiniBatchControlTest {
    @Test
    void generatedChangelogAndControlFlushesMatchFlinkThroughOneNativeTree(@TempDir Path directory) throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int trigger : List.of(1, 3, 7)) {
                var memory = new SharedAggregateRegionParityTest.Memory();
                try (var flink = SharedAggregateFlinkOracle.create(rocks, trigger);
                        var allocator = new RootAllocator(64L << 20);
                        var context = new NativeExecutionContext(
                                plan(trigger), memory, bindings(rocks, directory.resolve("db-" + trigger)));
                        var dispatcher = new ArrowNativePlanDispatcher(
                                context,
                                List.of(SharedAggregateFlinkOracle.INPUT),
                                SharedAggregateFlinkOracle.OUTPUT,
                                allocator)) {
                    var random = new Random(trigger);
                    var live = new ArrayList<GenericRowData>();
                    var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
                    // The dispatcher retains one typed empty VARCHAR port (its offset buffer).
                    long portBytes = allocator.getAllocatedMemory();
                    long inputs = 0, outputs = 0;
                    for (int arrival = 0; arrival < 7; arrival++) {
                        var rows = new ArrayList<RowData>();
                        int size = arrival == 2 ? 0 : arrival == 0 ? 1 : 23;
                        var kinds = new RowKind[size];
                        var present = new boolean[size];
                        var times = new long[size];
                        for (int i = 0; i < size; i++) {
                            GenericRowData row;
                            if (!live.isEmpty() && random.nextBoolean()) {
                                row = live.remove(random.nextInt(live.size()));
                                row.setRowKind(i % 2 == 0 ? RowKind.DELETE : RowKind.UPDATE_BEFORE);
                            } else {
                                row = GenericRowData.of(
                                        i % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(5)),
                                        i % 4 == 0 ? null : (long) random.nextInt(17) - 8);
                                row.setRowKind(i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                                live.add(GenericRowData.of(row.getField(0), row.getField(1)));
                            }
                            kinds[i] = row.getRowKind();
                            present[i] = i % 2 == 0;
                            times[i] = arrival * 100L + i;
                            rows.add(row);
                            flink.processElement(
                                    present[i] ? new StreamRecord<>(row, times[i]) : new StreamRecord<>(row));
                        }
                        var actual = new DataOutputSerializer(128);
                        try (var input = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)
                                .withEnvelope(kinds, present, times)) {
                            dispatcher.process(0, input, output -> serialize(output, serializer, actual));
                        }
                        if (arrival == 3) {
                            flink.processWatermark(new Watermark(123));
                            dispatcher.control(
                                    control(NativeStageControl.newBuilder()
                                            .setPlanNodeId(3)
                                            .setWatermarkMillis(123)),
                                    output -> serialize(output, serializer, actual));
                        } else if (arrival == 5) {
                            flink.getOperator().prepareSnapshotPreBarrier(77);
                            dispatcher.control(
                                    control(NativeStageControl.newBuilder()
                                            .setPlanNodeId(3)
                                            .setBeforeCheckpoint(77)),
                                    output -> serialize(output, serializer, actual));
                            for (int group = 0; group < 16; group++)
                                context.state().snapshot(3, group);
                        } else if (arrival == 6) {
                            flink.getOperator().finish();
                            dispatcher.control(
                                    control(NativeStageControl.newBuilder()
                                            .setPlanNodeId(3)
                                            .setEndInput(NativeControlEndInput.getDefaultInstance())),
                                    output -> serialize(output, serializer, actual));
                        }
                        var expected = new DataOutputSerializer(128);
                        var records = flink.extractOutputStreamRecords();
                        for (var record : records) {
                            assertThat(record.hasTimestamp()).isFalse();
                            serializer.serialize(record.getValue(), expected);
                        }
                        outputs += records.size();
                        inputs += rows.size();
                        flink.getOutput().clear();
                        assertThat(actual.getCopyOfBuffer())
                                .as("rocks=%s trigger=%s arrival=%s", rocks, trigger, arrival)
                                .isEqualTo(expected.getCopyOfBuffer());
                        assertThat(context.metricSnapshot())
                                .containsExactly(
                                        4, outputs, outputs, 3, inputs, outputs, 2, inputs, inputs, 1, 0, inputs);
                        assertThat(allocator.getAllocatedMemory()).isEqualTo(portBytes);
                    }
                }
                assertThat(memory.available()).isEqualTo(memory.limit());
            }
    }

    @Test
    void failedControlConsumerCancelsTheTreeAndRequiresRecovery() throws Exception {
        var memory = new SharedAggregateRegionParityTest.Memory();
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan(100_000), memory, bindings(false, null));
                var dispatcher = new ArrowNativePlanDispatcher(
                        context,
                        List.of(SharedAggregateFlinkOracle.INPUT),
                        SharedAggregateFlinkOracle.OUTPUT,
                        allocator)) {
            long portBytes = allocator.getAllocatedMemory();
            var rows = new ArrayList<RowData>();
            // More than one 2,048-row output chunk, within the fixture budget with AVG state.
            for (int i = 0; i < 3000; i++) rows.add(GenericRowData.of(StringData.fromString("key-" + i), (long) i));
            try (var input = ArrowRowDataBatch.transpose(rows, SharedAggregateFlinkOracle.INPUT, allocator)) {
                dispatcher.process(0, input, output -> {
                    throw new AssertionError("unexpected count flush");
                });
            }
            var failure = new IllegalStateException("downstream control consumer failed");
            var request = control(NativeStageControl.newBuilder()
                    .setPlanNodeId(3)
                    .setEndInput(NativeControlEndInput.getDefaultInstance()));
            assertThatThrownBy(() -> dispatcher.control(request, output -> {
                        assertThat(output.size()).isEqualTo(2048);
                        throw failure;
                    }))
                    .isSameAs(failure);
            assertThatThrownBy(() -> dispatcher.control(request, output -> {}))
                    .hasMessageContaining("requires recovery");
            assertThatThrownBy(() -> context.state().snapshot(3, 0))
                    .hasMessageContaining("active or failed invocation");
            assertThat(allocator.getAllocatedMemory()).isEqualTo(portBytes);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    @Test
    void malformedControlsAreRetryableAndUndrainedStateIsNotSnapshotable() throws Exception {
        var memory = new SharedAggregateRegionParityTest.Memory();
        try (var allocator = new RootAllocator(64L << 20);
                var context = new NativeExecutionContext(plan(10), memory, bindings(false, null));
                var dispatcher = new ArrowNativePlanDispatcher(
                        context,
                        List.of(SharedAggregateFlinkOracle.INPUT),
                        SharedAggregateFlinkOracle.OUTPUT,
                        allocator);
                var input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(StringData.fromString("key"), 1L)),
                        SharedAggregateFlinkOracle.INPUT,
                        allocator)) {
            dispatcher.process(0, input, output -> {
                throw new AssertionError("unexpected count flush");
            });
            assertThatThrownBy(() -> context.state().snapshot(3, 0)).hasMessageContaining("bundle control drain");
            for (byte[] invalid : List.of(
                    NativeControlInvocation.newBuilder()
                            .setProtocolVersion(99)
                            .build()
                            .toByteArray(),
                    control(NativeStageControl.newBuilder().setPlanNodeId(3)),
                    control(NativeStageControl.newBuilder().setPlanNodeId(999).setWatermarkMillis(1)),
                    NativeControlInvocation.newBuilder()
                            .setProtocolVersion(1)
                            .addStages(NativeStageControl.newBuilder()
                                    .setPlanNodeId(3)
                                    .setWatermarkMillis(1))
                            .addStages(NativeStageControl.newBuilder()
                                    .setPlanNodeId(3)
                                    .setWatermarkMillis(2))
                            .build()
                            .toByteArray())) {
                long available = memory.available();
                assertThatThrownBy(() -> dispatcher.control(invalid, output -> {
                            throw new AssertionError("invalid control emitted");
                        }))
                        .isInstanceOf(RuntimeException.class);
                assertThat(memory.available()).isEqualTo(available);
            }
            var rows = new ArrayList<RowKind>();
            dispatcher.control(
                    control(NativeStageControl.newBuilder().setPlanNodeId(3).setWatermarkMillis(1)), output -> {
                        for (int row = 0; row < output.size(); row++) rows.add(output.rowKind(row));
                    });
            assertThat(rows).containsExactly(RowKind.INSERT);
            for (int group = 0; group < 16; group++) context.state().snapshot(3, group);
        }
        assertThat(memory.available()).isEqualTo(memory.limit());
    }

    private static void serialize(ArrowRowDataBatch output, RowDataSerializer serializer, DataOutputSerializer target) {
        try {
            for (int row = 0; row < output.size(); row++) {
                assertThat(output.hasTimestamp(row)).isFalse();
                var value = output.rowView(row);
                value.setRowKind(output.rowKind(row));
                serializer.serialize(value, target);
            }
        } catch (java.io.IOException error) {
            throw new RuntimeException(error);
        }
    }

    private static byte[] control(NativeStageControl.Builder stage) {
        return NativeControlInvocation.newBuilder()
                .setProtocolVersion(1)
                .addStages(stage)
                .build()
                .toByteArray();
    }

    static byte[] plan(int trigger) throws Exception {
        var plan = NativePlan.parseFrom(SharedAggregateRegionParityTest.plan());
        var root = plan.getRoot();
        var group = root.getCalc().getInput();
        var mini =
                NativePlan.parseFrom(GeneratedRawMiniBatchParityTest.plan(trigger))
                        .getRoot()
                        .getGroupAggregate()
                        .toBuilder()
                        .setInput(group.getGroupAggregate().getInput());
        return plan.toBuilder()
                .setRoot(root.toBuilder()
                        .setCalc(root.getCalc().toBuilder()
                                .setInput(group.toBuilder().setGroupAggregate(mini))))
                .build()
                .toByteArray();
    }

    private static byte[] bindings(boolean rocks, Path path) {
        return NativeStateResources.serialize(List.of(
                rocks
                        ? NativeStateResources.rocksDb(3, 16, 0, 15, path, 8L << 20)
                        : NativeStateResources.memory(3, 16, 0, 15)));
    }
}
