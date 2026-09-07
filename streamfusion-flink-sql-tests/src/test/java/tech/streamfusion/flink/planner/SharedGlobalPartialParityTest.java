/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.planner.GlobalPartialFixtures.*;

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
import tech.streamfusion.flink.arrow.ArrowLocalGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeLocalGroupAggregateBridge;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativeControlEndInput;
import tech.streamfusion.proto.plan.v1.NativeControlInvocation;
import tech.streamfusion.proto.plan.v1.NativeStageControl;

/**
 * One-record local bundles produce opaque input fixtures for the global shared tree. Their merging
 * must match the original SQL-planned raw Flink bundle at every receiving count/control boundary.
 * This is not evidence that the local producer or a complete two-phase SQL plan is fused/selected.
 */
class SharedGlobalPartialParityTest {

    @Test
    void opaqueSignedPartialsMatchFlinkThroughTheCommonNativeTree(@TempDir Path directory) throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int trigger : List.of(1, 3, 7))
                for (int seed = 0; seed < 3; seed++) {
                    var memory = new SharedAggregateRegionParityTest.Memory();
                    long local = NativeLocalGroupAggregateBridge.create(localPlan(), memory);
                    try (var flink = SharedAggregateFlinkOracle.create(rocks, trigger);
                            var allocator = new RootAllocator(64L << 20);
                            var context = new NativeExecutionContext(
                                    globalPlan(trigger),
                                    memory,
                                    NativeStateResources.serialize(List.of(
                                            rocks
                                                    ? NativeStateResources.rocksDb(
                                                            3,
                                                            16,
                                                            0,
                                                            15,
                                                            directory.resolve(trigger + "-" + seed),
                                                            8L << 20)
                                                    : NativeStateResources.memory(3, 16, 0, 15))));
                            var dispatcher = new ArrowNativePlanDispatcher(
                                    context, List.of(PARTIAL), SharedAggregateFlinkOracle.OUTPUT, allocator)) {
                        var random = new Random(seed);
                        var live = new ArrayList<GenericRowData>();
                        var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
                        long portBytes = allocator.getAllocatedMemory();
                        long inputs = 0, outputs = 0;
                        for (int arrival = 0; arrival < 7; arrival++) {
                            var rows = new ArrayList<RowData>();
                            var kinds = new RowKind[arrival == 2 ? 0 : arrival == 0 ? 1 : 23];
                            for (int i = 0; i < kinds.length; i++) {
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
                                rows.add(row);
                                kinds[i] = row.getRowKind();
                                flink.processElement(new StreamRecord<>(row, arrival * 100L + i));
                            }
                            var actual = new DataOutputSerializer(128);
                            try (var raw = ArrowRowDataBatch.transpose(
                                                    rows, SharedAggregateFlinkOracle.INPUT, allocator)
                                            .withRowKinds(kinds);
                                    var partials = ArrowLocalGroupAggregateCDataBridge.execute(
                                            local, raw, null, true, PARTIAL, allocator, memory)) {
                                assertThat(partials.size()).isEqualTo(rows.size());
                                dispatcher.process(0, partials, output -> serialize(output, serializer, actual));
                            }
                            var control = NativeStageControl.newBuilder().setPlanNodeId(3);
                            if (arrival == 3) {
                                flink.processWatermark(new Watermark(123));
                                control.setWatermarkMillis(123);
                            } else if (arrival == 5) {
                                flink.getOperator().prepareSnapshotPreBarrier(77);
                                control.setBeforeCheckpoint(77);
                            } else if (arrival == 6) {
                                flink.getOperator().finish();
                                control.setEndInput(NativeControlEndInput.getDefaultInstance());
                            }
                            if (arrival == 3 || arrival == 5 || arrival == 6) {
                                dispatcher.control(
                                        NativeControlInvocation.newBuilder()
                                                .setProtocolVersion(1)
                                                .addStages(control)
                                                .build()
                                                .toByteArray(),
                                        output -> serialize(output, serializer, actual));
                            }
                            if (arrival == 5) {
                                for (int group = 0; group < 16; group++)
                                    context.state().snapshot(3, group);
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
                                    .as("rocks=%s trigger=%s seed=%s arrival=%s", rocks, trigger, seed, arrival)
                                    .isEqualTo(expected.getCopyOfBuffer());
                            assertThat(context.metricSnapshot())
                                    .containsExactly(
                                            4, outputs, outputs, 3, inputs, outputs, 2, inputs, inputs, 1, 0, inputs);
                            assertThat(allocator.getAllocatedMemory()).isEqualTo(portBytes);
                        }
                    } finally {
                        NativeLocalGroupAggregateBridge.destroy(local);
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                }
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
}
