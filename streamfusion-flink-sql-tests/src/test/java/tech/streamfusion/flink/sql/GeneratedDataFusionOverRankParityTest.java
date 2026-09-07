/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.*;
import tech.streamfusion.nativebridge.NativeBoundedRankBridge;
import tech.streamfusion.nativebridge.NativeOverAggregateBridge;
import tech.streamfusion.proto.plan.v1.*;

class GeneratedDataFusionOverRankParityTest extends NativeComputeParitySupport {
    @Test
    void overFramesMatchFlinkSqlWithNullsPeersAndSplitInput(@TempDir Path directory) throws Exception {
        var output = RowType.of(
                new IntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType());
        for (boolean rowsFrame : List.of(false, true))
            for (int seed = 0; seed < 2; seed++) {
                var rows = generated(seed);
                // Make ROWS ordering total so different physical sort tie orders cannot change the frame.
                if (rowsFrame)
                    for (int i = 0; i < rows.size(); i++)
                        ((org.apache.flink.table.data.GenericRowData) rows.get(i)).setField(1, (long) i);
                var over = OverAggregate.newBuilder()
                        .setInputSchema(schema(INPUT))
                        .setOutputSchema(schema(output))
                        .addPartitionKeyIndices(0)
                        .setOrderKeyIndex(1)
                        .setRowsFrame(rowsFrame)
                        .setPrecedingOffset(rowsFrame ? 3 : 2)
                        .setSortAscending(true)
                        .setSortNullsLast(false)
                        .setBoundedFinalOutput(true)
                        .setTimeAttribute(OverTimeAttribute.OVER_TIME_ATTRIBUTE_NON_TIME);
                for (var fn : List.of(
                        AggregateFunction.AGGREGATE_FUNCTION_SUM,
                        AggregateFunction.AGGREGATE_FUNCTION_COUNT,
                        AggregateFunction.AGGREGATE_FUNCTION_MIN)) over.addAggregateCalls(call(fn, true));
                byte[] plan = plan(Operator.newBuilder().setOverAggregate(over));
                String frame = " OVER (PARTITION BY k ORDER BY o " + (rowsFrame ? "ROWS" : "RANGE")
                        + " BETWEEN 2 PRECEDING AND CURRENT ROW)";
                var expected = flink(
                        "SELECT k,o,v,SUM(v)" + frame + ",COUNT(v)" + frame + ",MIN(v)" + frame + " FROM "
                                + relation(rows, "t"),
                        output);
                for (boolean rocks : List.of(false, true)) {
                    var memory = new Memory();
                    long handle = rocks
                            ? NativeOverAggregateBridge.createRocksDb(
                                    plan,
                                    16,
                                    0,
                                    15,
                                    directory.resolve("over-" + rowsFrame + "-" + seed),
                                    8L << 20,
                                    memory)
                            : NativeOverAggregateBridge.create(plan, 16, 0, 15, memory);
                    var actual = new ArrayList<byte[]>();
                    try (var allocator = new RootAllocator(128L << 20)) {
                        for (int offset = 0; offset < rows.size(); offset += 7)
                            try (var input = ArrowRowDataBatch.transpose(
                                            rows.subList(offset, Math.min(offset + 7, rows.size())), INPUT, allocator);
                                    var result = ArrowOverAggregateCDataBridge.process(
                                            handle, input, null, false, 0, output, allocator, memory)) {
                                assertThat(result.size()).isZero();
                            }
                        while (true)
                            try (var result = ArrowOverAggregateCDataBridge.finish(handle, output, allocator, memory)) {
                                if (result.size() == 0) break;
                                append(result, output, actual);
                            }
                    } finally {
                        NativeOverAggregateBridge.destroy(handle);
                    }
                    actual.sort(Arrays::compareUnsigned);
                    assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
                    assertThat(memory.reserved).isZero();
                }
            }
    }

    @Test
    void rankMatchesFlinkSqlAcrossPeerAndPartitionBatchBoundaries() throws Exception {
        var output = RowType.of(new IntType(), new BigIntType(), new BigIntType(), new BigIntType());
        var rank = BoundedRank.newBuilder()
                .setInputSchema(schema(INPUT))
                .setOutputSchema(schema(output))
                .addPartitionKeyIndices(0)
                .addSortKeyIndices(1)
                .setRankStart(2)
                .setRankEnd(5)
                .setOutputRankNumber(true);
        for (int seed = 0; seed < 3; seed++) {
            var rows = generated(seed);
            var expected = flink(
                    "SELECT k,o,v,r FROM (SELECT k,o,v,RANK() OVER (PARTITION BY k ORDER BY o) AS r FROM "
                            + relation(rows, "t") + ") WHERE r BETWEEN 2 AND 5",
                    output);
            rows.sort(Comparator.comparingInt((RowData row) -> row.getInt(0)).thenComparingLong(row -> row.getLong(1)));
            var memory = new Memory();
            long handle =
                    NativeBoundedRankBridge.create(plan(Operator.newBuilder().setBoundedRank(rank)), memory);
            var actual = new ArrayList<byte[]>();
            try (var allocator = new RootAllocator(128L << 20)) {
                for (int offset = 0; offset < rows.size(); offset += 3)
                    try (var input = ArrowRowDataBatch.transpose(
                                    rows.subList(offset, Math.min(offset + 3, rows.size())), INPUT, allocator);
                            var result =
                                    ArrowBoundedRankCDataBridge.process(handle, input, output, allocator, memory)) {
                        append(result, output, actual);
                    }
                assertThat(NativeBoundedRankBridge.statistics(handle)[1]).isEqualTo(actual.size());
            } finally {
                NativeBoundedRankBridge.destroy(handle);
            }
            actual.sort(Arrays::compareUnsigned);
            assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
            assertThat(memory.reserved).isZero();
        }
    }
}
