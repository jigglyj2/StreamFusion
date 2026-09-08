/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowRankCDataBridge;
import tech.streamfusion.nativebridge.NativeWindowRankBridge;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.WindowRank;

class GeneratedOrderedWindowRankParityTest extends NativeComputeParitySupport {
    @Test
    void orderedCandidatesMatchFlinkAfterRetractionsAndCrossBackendRestore(@TempDir Path directory) throws Exception {
        var inputType = RowType.of(new IntType(), new TimestampType(3), new BigIntType());
        var output = RowType.of(new IntType(), new TimestampType(3), new BigIntType(), new BigIntType());
        var plan = plan(Operator.newBuilder()
                .setWindowRank(WindowRank.newBuilder()
                        .setInputSchema(schema(inputType))
                        .addPartitionKeyIndices(0)
                        .addSortKeyIndices(2)
                        .addSortAscending(false)
                        .addSortNullsLast(false)
                        .setWindowEndIndex(1)
                        .setRankStart(2)
                        .setRankEnd(4)
                        .setOutputRankNumber(true)
                        .setInputChangelog(true)));
        for (int seed = 0; seed < 3; seed++) {
            var rows = generated(seed);
            for (var row : rows) ((GenericRowData) row).setField(1, row.getLong(1) + 1_000);
            // Retract a row inserted before the checkpoint, including a null sort key.
            var removed = rows.get(6);
            var delete = GenericRowData.of(removed.getInt(0), removed.getLong(1), null);
            delete.setRowKind(RowKind.DELETE);
            var finalRows = new ArrayList<>(rows);
            finalRows.remove(6);
            var expected = flink(
                    "SELECT k,o,v,r FROM (SELECT k,o,v,ROW_NUMBER() OVER "
                            + "(PARTITION BY k,o ORDER BY v DESC NULLS FIRST) AS r FROM "
                            + timestampRelation(finalRows) + ") WHERE r BETWEEN 2 AND 4",
                    output);
            for (boolean rocksFirst : List.of(false, true)) {
                var memory = new Memory();
                long handle = create(plan, rocksFirst, directory.resolve(seed + "-" + rocksFirst + "-before"), memory);
                var actual = new ArrayList<byte[]>();
                try (var allocator = new RootAllocator(128L << 20)) {
                    for (int offset = 0; offset < rows.size(); offset += 7) {
                        if (offset == 14) {
                            var snapshots = new ArrayList<byte[]>();
                            for (int group = 0; group < 16; group++)
                                snapshots.add(NativeWindowRankBridge.snapshot(handle, group));
                            NativeWindowRankBridge.destroy(handle);
                            handle = 0;
                            assertThat(memory.reserved).isZero();
                            handle = create(
                                    plan, !rocksFirst, directory.resolve(seed + "-" + rocksFirst + "-after"), memory);
                            for (int group = 0; group < 16; group++)
                                NativeWindowRankBridge.restore(handle, group, snapshots.get(group));
                        }
                        try (var input = ArrowRowDataBatch.transpose(
                                        timestampRows(rows.subList(offset, Math.min(offset + 7, rows.size()))),
                                        inputType,
                                        allocator);
                                var result = ArrowWindowRankCDataBridge.process(
                                        handle, input, null, output, allocator, memory)) {
                            assertThat(result.size()).isZero();
                        }
                    }
                    try (var input = ArrowRowDataBatch.transpose(
                                            timestampRows(List.<RowData>of(delete)), inputType, allocator)
                                    .withRowKinds(new RowKind[] {RowKind.DELETE});
                            var result = ArrowWindowRankCDataBridge.process(
                                    handle, input, null, output, allocator, memory)) {
                        assertThat(result.size()).isZero();
                    }
                    try (var result = ArrowWindowRankCDataBridge.advance(handle, 2_000, output, allocator, memory)) {
                        append(result, output, actual);
                    }
                    assertThat(NativeWindowRankBridge.lateRecordCount(handle)).isZero();
                    assertThat(NativeWindowRankBridge.statistics(handle)[5]).isZero();
                } finally {
                    if (handle != 0) NativeWindowRankBridge.destroy(handle);
                }
                actual.sort(Arrays::compareUnsigned);
                assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
                assertThat(memory.reserved).isZero();
            }
        }
    }

    private static List<RowData> timestampRows(List<RowData> rows) {
        var result = new ArrayList<RowData>();
        for (var row : rows) {
            var value = GenericRowData.of(
                    row.getInt(0),
                    TimestampData.fromEpochMillis(row.getLong(1)),
                    row.isNullAt(2) ? null : row.getLong(2));
            value.setRowKind(row.getRowKind());
            result.add(value);
        }
        return result;
    }

    private static String timestampRelation(List<RowData> rows) {
        var values = new ArrayList<String>();
        for (var row : rows) {
            var timestamp = TimestampData.fromEpochMillis(row.getLong(1))
                    .toLocalDateTime()
                    .toString()
                    .replace('T', ' ');
            values.add("(" + row.getInt(0) + ", TIMESTAMP '" + timestamp + "', CAST("
                    + (row.isNullAt(2) ? "NULL" : row.getLong(2)) + " AS BIGINT))");
        }
        return "(VALUES " + String.join(",", values) + ") AS t(k,o,v)";
    }

    private static long create(byte[] plan, boolean rocks, Path directory, Memory memory) {
        return rocks
                ? NativeWindowRankBridge.createRocksDb(plan, 16, 0, 15, directory, 8L << 20, memory)
                : NativeWindowRankBridge.create(plan, 16, 0, 15, memory);
    }
}
