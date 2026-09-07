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
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.GroupAggregate;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class SharedAggregateRegionParityTest {
    @Test
    void generatedChangelogsAndTimestampsMatchSqlPlannedFlinkAcrossBackendRestore(@TempDir Path directory)
            throws Exception {
        for (boolean rocksFirst : List.of(false, true))
            for (int seed = 0; seed < 3; seed++) {
                var memory = new Memory();
                var serializer = new RowDataSerializer(SharedAggregateFlinkOracle.OUTPUT);
                byte[][] snapshot = new byte[16][];
                var random = new Random(seed);
                var live = new ArrayList<GenericRowData>();
                try (var flink = SharedAggregateFlinkOracle.create();
                        var allocator = new RootAllocator(64L << 20)) {
                    for (int phase = 0; phase < 2; phase++) {
                        boolean rocks = phase == 0 ? rocksFirst : !rocksFirst;
                        byte[] bindings = NativeStateResources.serialize(List.of(
                                rocks
                                        ? NativeStateResources.rocksDb(
                                                3,
                                                16,
                                                0,
                                                15,
                                                directory.resolve(rocksFirst + "-" + seed + "-" + phase),
                                                8L << 20)
                                        : NativeStateResources.memory(3, 16, 0, 15)));
                        try (var context = new NativeExecutionContext(plan(), memory, bindings);
                                var dispatcher = new ArrowNativePlanDispatcher(
                                        context,
                                        List.of(SharedAggregateFlinkOracle.INPUT),
                                        SharedAggregateFlinkOracle.OUTPUT,
                                        allocator)) {
                            if (phase != 0)
                                for (int group = 0; group < 16; group++)
                                    context.state().restore(3, group, snapshot[group]);
                            long inputs = 0;
                            long outputs = 0;
                            for (int arrival = 0; arrival < 5; arrival++) {
                                var rows = new ArrayList<RowData>();
                                var kinds = new RowKind[arrival == 2 ? 0 : 24];
                                var present = new boolean[kinds.length];
                                var times = new long[kinds.length];
                                var expected = new DataOutputSerializer(128);
                                var actual = new DataOutputSerializer(128);
                                var expectedTimes = new ArrayList<Long>();
                                var actualTimes = new ArrayList<Long>();
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
                                    present[i] = i % 3 != 0;
                                    times[i] = 1000L * phase + 100L * arrival + i;
                                    flink.processElement(
                                            present[i] ? new StreamRecord<>(row, times[i]) : new StreamRecord<>(row));
                                }
                                for (var record : flink.extractOutputStreamRecords()) {
                                    serializer.serialize(record.getValue(), expected);
                                    expectedTimes.add(record.hasTimestamp() ? record.getTimestamp() : null);
                                }
                                flink.getOutput().clear();
                                try (var input = ArrowRowDataBatch.transpose(
                                                rows, SharedAggregateFlinkOracle.INPUT, allocator)
                                        .withEnvelope(kinds, present, times)) {
                                    dispatcher.process(0, input, batch -> {
                                        try {
                                            for (int i = 0; i < batch.size(); i++) {
                                                var row = batch.rowView(i);
                                                row.setRowKind(batch.rowKind(i));
                                                serializer.serialize(row, actual);
                                                actualTimes.add(batch.hasTimestamp(i) ? batch.timestamp(i) : null);
                                            }
                                        } catch (java.io.IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                                }
                                assertThat(actual.getCopyOfBuffer()).isEqualTo(expected.getCopyOfBuffer());
                                assertThat(actualTimes).isEqualTo(expectedTimes);
                                inputs += rows.size();
                                outputs += expectedTimes.size();
                                assertThat(context.metricSnapshot())
                                        .containsExactly(
                                                4, outputs, outputs, 3, inputs, outputs, 2, inputs, inputs, 1, 0,
                                                inputs);
                            }
                            for (int group = 0; group < 16; group++)
                                snapshot[group] = context.state().snapshot(3, group);
                        }
                        assertThat(memory.available()).isEqualTo(memory.limit());
                        assertThat(allocator.getAllocatedMemory()).isZero();
                    }
                }
            }
    }

    static byte[] plan() {
        var input = Operator.newBuilder()
                .setPlanNodeId(1)
                .setInput(Input.newBuilder())
                .build();
        var group = GroupAggregate.newBuilder()
                .setInput(calc(2, input, 2))
                .addGroupingIndices(0)
                .setGenerateUpdateBefore(true)
                .setInputChangelog(true);
        for (var function : List.of(
                AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR,
                AggregateFunction.AGGREGATE_FUNCTION_SUM,
                AggregateFunction.AGGREGATE_FUNCTION_MIN,
                AggregateFunction.AGGREGATE_FUNCTION_MAX)) {
            boolean count = function == AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR;
            var type = LogicalType.newBuilder()
                    .setBigint(EmptyType.getDefaultInstance())
                    .setNullable(!count)
                    .build();
            var call = AggregateCall.newBuilder()
                    .setFunction(function)
                    .setOutputType(type)
                    .setRetractable(true);
            if (!count) call.setInputIndex(1).setInputType(type);
            group.addAggregateCalls(call);
        }
        var aggregate =
                Operator.newBuilder().setPlanNodeId(3).setGroupAggregate(group).build();
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(calc(4, aggregate, 5))
                .build()
                .toByteArray();
    }

    private static Operator calc(long id, Operator input, int width) {
        var calc = Calc.newBuilder().setInput(input).setPreserveInputEnvelope(true);
        for (int i = 0; i < width; i++)
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(i)));
        return Operator.newBuilder().setPlanNodeId(id).setCalc(calc).build();
    }

    static final class Memory implements NativeMemoryManager {
        private long reserved;

        public synchronized boolean tryReserve(long bytes) {
            if (bytes < 0 || bytes > limit() - reserved) return false;
            reserved += bytes;
            return true;
        }

        public synchronized void release(long bytes) {
            if (bytes < 0 || bytes > reserved) throw new IllegalStateException("invalid release");
            reserved -= bytes;
        }

        public synchronized long available() {
            return limit() - reserved;
        }

        public long limit() {
            return 64L << 20;
        }
    }
}
