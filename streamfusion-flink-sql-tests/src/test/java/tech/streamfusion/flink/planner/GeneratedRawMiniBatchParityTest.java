/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeGroupAggregateBridge;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Retained-kernel parity; does not bypass or claim shared-plan mini-batch admission. */
class GeneratedRawMiniBatchParityTest {
    @Test
    void generatedOrderedChangelogMatchesSqlPlannedFlinkAndBatchesBackendIo(@TempDir Path directory) throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int trigger : List.of(1, 3, 7))
                for (int seed = 0; seed < 3; seed++) {
                    var memory = new SharedAggregateRegionParityTest.Memory();
                    long handle = rocks
                            ? NativeGroupAggregateBridge.createRocksDb(
                                    plan(trigger), 16, 0, 15, directory.resolve(trigger + "-" + seed), 8L << 20, memory)
                            : NativeGroupAggregateBridge.create(plan(trigger), 16, 0, 15, memory);
                    try (var flink = SharedAggregateFlinkOracle.create(rocks, trigger);
                            var allocator = new RootAllocator(64L << 20)) {
                        var random = new Random(seed);
                        var live = new ArrayList<GenericRowData>();
                        var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
                        for (int arrival = 0; arrival < 6; arrival++) {
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
                                flink.processElement(new StreamRecord<>(row));
                            }
                            long[] before = NativeGroupAggregateBridge.statistics(handle);
                            var actual = new DataOutputSerializer(128);
                            try (var input = ArrowRowDataBatch.transpose(
                                                    rows, SharedAggregateFlinkOracle.INPUT, allocator)
                                            .withRowKinds(kinds);
                                    var output = ArrowGroupAggregateCDataBridge.execute(
                                            handle,
                                            input,
                                            null,
                                            true,
                                            SharedAggregateFlinkOracle.OUTPUT,
                                            allocator,
                                            memory)) {
                                serialize(output, serializer, actual);
                            }
                            long[] after = NativeGroupAggregateBridge.statistics(handle);
                            assertThat(after[0] - before[0]).isBetween(0L, 1L);
                            assertThat(after[1] - before[1]).isBetween(0L, 1L);
                            var expected = new DataOutputSerializer(128);
                            for (var record : flink.extractOutputStreamRecords())
                                serializer.serialize(record.getValue(), expected);
                            flink.getOutput().clear();
                            assertThat(actual.getCopyOfBuffer())
                                    .as("rocks=%s trigger=%s seed=%s arrival=%s", rocks, trigger, seed, arrival)
                                    .isEqualTo(expected.getCopyOfBuffer());
                        }
                        flink.processWatermark(new Watermark(1000));
                        var expected = new DataOutputSerializer(128);
                        for (var record : flink.extractOutputStreamRecords())
                            serializer.serialize(record.getValue(), expected);
                        var actual = new DataOutputSerializer(128);
                        try (var output = ArrowGroupAggregateCDataBridge.finishBundle(
                                handle, SharedAggregateFlinkOracle.OUTPUT, allocator, memory)) {
                            serialize(output, serializer, actual);
                        }
                        assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                        assertThat(allocator.getAllocatedMemory()).isZero();
                    } finally {
                        NativeGroupAggregateBridge.destroy(handle);
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                }
    }

    private static void serialize(ArrowRowDataBatch output, RowDataSerializer serializer, DataOutputSerializer target)
            throws Exception {
        for (int row = 0; row < output.size(); row++) {
            var value = output.rowView(row);
            value.setRowKind(output.rowKind(row));
            serializer.serialize(value, target);
        }
    }

    static byte[] plan(int trigger) throws Exception {
        var aggregate = NativePlan.parseFrom(SharedAggregateRegionParityTest.plan())
                .getRoot()
                .getCalc()
                .getInput()
                .getGroupAggregate()
                .toBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setMiniBatchSize(trigger)
                .setInputSchema(schema(SharedAggregateFlinkOracle.INPUT))
                .setOutputSchema(schema(SharedAggregateFlinkOracle.OUTPUT));
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder().setGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static tech.streamfusion.proto.plan.v1.Schema schema(RowType type) {
        var schema = tech.streamfusion.proto.plan.v1.Schema.newBuilder();
        for (var field : type.getFields())
            schema.addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        return schema.build();
    }
}
