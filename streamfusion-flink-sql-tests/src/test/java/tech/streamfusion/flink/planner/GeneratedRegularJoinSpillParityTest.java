/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowNativePlanBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.flink.planner.join.StreamFusionRegularJoinPlan;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Exercises the production state-resource contract and actual Arrow C Stream under pressure. */
class GeneratedRegularJoinSpillParityTest {
    private static final RowType INPUT = RowType.of(new BigIntType(false), new VarCharType());
    private static final RowType OUTPUT =
            RowType.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType());

    @Test
    void generatedCompleteChangelogsAndRecordCountsMatchFlinkWhileHistoryExceedsWorkspace(@TempDir Path temporary)
            throws Exception {
        byte[] plan = StreamFusionNativeRegionTranslator.identifyStage(
                StreamFusionRegularJoinPlan.create(
                        INPUT, INPUT, new int[] {0}, new int[] {0}, new boolean[] {true}, FlinkJoinType.INNER, null),
                2);
        long node = NativePlan.parseFrom(plan).getRoot().getPlanNodeId();
        for (boolean rocks : List.of(false, true)) {
            for (int side = 0; side < 2; side++) {
                for (int seed = 0; seed < 2; seed++) {
                    Path spill = Files.createDirectory(temporary.resolve("spill-" + rocks + "-" + side + "-" + seed));
                    var memory = new SharedAggregateRegionParityTest.Memory();
                    var binding = rocks
                            ? NativeStateResources.rocksDb(
                                    node, 16, 0, 15, temporary.resolve("db-" + side + "-" + seed), 1L << 20)
                            : NativeStateResources.memory(node, 16, 0, 15);
                    String code =
                            "public class SpillJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                                    + "implements org.apache.flink.table.runtime.generated.JoinCondition { public SpillJoinCondition(Object[] refs) {} "
                                    + "public boolean apply(org.apache.flink.table.data.RowData left, org.apache.flink.table.data.RowData right) { return left.getLong(0) == right.getLong(0); }}";
                    try (var oracle = RegularJoinFlinkHarness.create(
                                    INPUT,
                                    OUTPUT,
                                    new GeneratedJoinCondition("SpillJoinCondition", code, new Object[0]),
                                    rocks,
                                    new OperatorID(),
                                    "spill parity");
                            var allocator = new RootAllocator(128L << 20);
                            var context = new NativeExecutionContext(
                                    plan, memory, NativeStateResources.serialize(List.of(binding), List.of(spill)));
                            var empty = ArrowRowDataBatch.empty(INPUT, allocator)) {
                        var edge = new ArrowNativePlanBridge(context, OUTPUT, allocator);
                        var random = new Random(719 + seed);
                        long inputs = 0;
                        long outputs = 0;
                        // More than 6 MiB of historical payload, admitted in ordinary input batches.
                        for (int start = 0; start < 1501; start += 64) {
                            List<RowData> rows = new ArrayList<>();
                            for (int i = start; i < Math.min(start + 64, 1501); i++) {
                                rows.add(row(
                                        "wide-" + random.nextInt(19) + "x".repeat(4096),
                                        i % 2 == 0 ? RowKind.INSERT : RowKind.UPDATE_AFTER));
                            }
                            compare(oracle, edge, allocator, empty, 1 - side, rows, null);
                            inputs += rows.size();
                        }
                        List<RowData> probe = List.of(
                                row("probe", RowKind.INSERT),
                                row("probe", RowKind.UPDATE_AFTER),
                                row("probe", RowKind.DELETE),
                                row("probe", RowKind.UPDATE_BEFORE),
                                row("absent", RowKind.DELETE));
                        long pressure = memory.available() - (4L << 20);
                        assertThat(memory.tryReserve(pressure)).isTrue();
                        try {
                            outputs += compare(oracle, edge, allocator, empty, side, probe, spill);
                        } finally {
                            memory.release(pressure);
                        }
                        inputs += probe.size();
                        long[] metrics = context.metricSnapshot();
                        assertThat(metrics[0]).isEqualTo(node);
                        assertThat(metrics[1]).isEqualTo(inputs);
                        assertThat(metrics[2]).isEqualTo(outputs);
                        assertThat(oracle.group()
                                        .getIOMetricGroup()
                                        .getNumRecordsInCounter()
                                        .getCount())
                                .isEqualTo(inputs);
                        assertThat(oracle.group()
                                        .getIOMetricGroup()
                                        .getNumRecordsOutCounter()
                                        .getCount())
                                .isEqualTo(outputs);
                        // Exhaustion has released the prepared file; only a manager directory may remain.
                        try (var files = Files.walk(spill)) {
                            assertThat(files.noneMatch(Files::isRegularFile)).isTrue();
                        }
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                    try (var files = Files.list(spill)) {
                        assertThat(files.count()).isZero();
                    }
                }
            }
        }
    }

    private static long compare(
            FlinkRegularJoinMetricOracle oracle,
            ArrowNativePlanBridge edge,
            RootAllocator allocator,
            ArrowRowDataBatch empty,
            int side,
            List<RowData> rows,
            Path expectedSpill)
            throws Exception {
        var serializer = new RowDataSerializer(INPUT);
        for (var row : rows) oracle.accept(side, new StreamRecord<>(serializer.copy(row)));
        List<byte[]> expected = new ArrayList<>();
        for (var event : oracle.drain()) expected.add(bytes(((StreamRecord<RowData>) event).getValue()));
        List<byte[]> actual = new ArrayList<>();
        boolean sawSpill = false;
        try (var input = ArrowRowDataBatch.transpose(rows, INPUT, allocator)
                        .withRowKinds(rows.stream().map(RowData::getRowKind).toArray(RowKind[]::new));
                var stream = edge.executeStream(side == 0 ? List.of(input, empty) : List.of(empty, input))) {
            ArrowRowDataBatch next;
            while ((next = stream.next()) != null) {
                try (var output = next) {
                    assertThat(output.size()).isBetween(1, 4096);
                    for (int i = 0; i < output.size(); i++) {
                        var row = output.rowView(i);
                        row.setRowKind(output.rowKind(i));
                        actual.add(bytes(row));
                    }
                    if (expectedSpill != null) {
                        try (var files = Files.walk(expectedSpill)) {
                            sawSpill |= files.anyMatch(Files::isRegularFile);
                        }
                    }
                }
            }
        }
        if (expectedSpill != null)
            assertThat(sawSpill).as("exercise actual prepared history replay").isTrue();
        actual.sort(Arrays::compareUnsigned);
        expected.sort(Arrays::compareUnsigned);
        assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++)
            assertThat(actual.get(i)).as("changelog record %s", i).isEqualTo(expected.get(i));
        return actual.size();
    }

    private static RowData row(String value, RowKind kind) {
        var row = GenericRowData.of(1L, StringData.fromString(value));
        row.setRowKind(kind);
        return row;
    }

    private static byte[] bytes(RowData row) throws Exception {
        var output = new DataOutputSerializer(64);
        new RowDataSerializer(OUTPUT).serialize(row, output);
        return output.getCopyOfBuffer();
    }
}
