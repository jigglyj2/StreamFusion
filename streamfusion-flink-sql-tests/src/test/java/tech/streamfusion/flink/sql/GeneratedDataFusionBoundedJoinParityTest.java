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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRegularJoinCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;
import tech.streamfusion.proto.plan.v1.*;

class GeneratedDataFusionBoundedJoinParityTest extends NativeComputeParitySupport {
    @Test
    void boundedJoinsMatchFlinkSqlAfterRetractionsOnBothBackends(@TempDir Path directory) throws Exception {
        var both = RowType.of(
                new IntType(), new BigIntType(), new BigIntType(), new IntType(), new BigIntType(), new BigIntType());
        byte[] exchange = NativeExchangePlanSerializer.hash(INPUT, new int[] {0}, 16);
        var modes = List.of(
                RegularJoinType.REGULAR_JOIN_TYPE_INNER,
                RegularJoinType.REGULAR_JOIN_TYPE_LEFT,
                RegularJoinType.REGULAR_JOIN_TYPE_RIGHT,
                RegularJoinType.REGULAR_JOIN_TYPE_FULL,
                RegularJoinType.REGULAR_JOIN_TYPE_SEMI,
                RegularJoinType.REGULAR_JOIN_TYPE_ANTI);
        for (int mode = 0; mode < modes.size(); mode++) {
            var left = generated(mode);
            var right = generated(mode + 10);
            ((GenericRowData) left.get(1)).setField(0, null);
            ((GenericRowData) right.get(1)).setField(0, null);
            var output = mode >= 4 ? INPUT : both;
            var leftSql = relation(left.subList(1, left.size()), "l");
            var rightSql = relation(right, "r");
            String sql = mode == 4
                    ? "SELECT l.k,l.o,l.v FROM " + leftSql + " WHERE l.k IN (SELECT r.k FROM " + rightSql + ")"
                    : mode == 5
                            ? "SELECT l.k,l.o,l.v FROM " + leftSql + " LEFT JOIN " + rightSql
                                    + " ON l.k = r.k WHERE r.k IS NULL"
                            : "SELECT l.k,l.o,l.v,r.k,r.o,r.v FROM " + leftSql + " "
                                    + List.of("INNER", "LEFT", "RIGHT", "FULL OUTER")
                                            .get(mode)
                                    + " JOIN " + rightSql + " ON l.k = r.k";
            var expected = flink(sql, output);
            byte[] plan = plan(Operator.newBuilder()
                    .setRegularJoin(RegularJoin.newBuilder()
                            .setLeftInput(Operator.newBuilder()
                                    .setInput(Input.newBuilder().setInputIndex(0)))
                            .setRightInput(Operator.newBuilder()
                                    .setInput(Input.newBuilder().setInputIndex(1)))
                            .setLeftSchema(schema(INPUT))
                            .setRightSchema(schema(INPUT))
                            .addLeftKeyIndices(0)
                            .addRightKeyIndices(0)
                            .addFilterNulls(true)
                            .setJoinType(modes.get(mode))
                            .setBoundedFinalOutput(true)));
            for (boolean rocks : List.of(false, true)) {
                var memory = new Memory();
                long handle = rocks
                        ? NativeRegularJoinBridge.createRocksDb(
                                plan, 16, 0, 15, directory.resolve("join-" + mode), 8L << 20, memory)
                        : NativeRegularJoinBridge.create(plan, 16, 0, 15, memory);
                var actual = new ArrayList<byte[]>();
                try (var allocator = new RootAllocator(128L << 20)) {
                    ingest(handle, 0, left, exchange, allocator, memory);
                    ingest(handle, 1, right, exchange, allocator, memory);
                    var deleted = (GenericRowData) left.get(0);
                    deleted.setRowKind(RowKind.DELETE);
                    ingest(handle, 0, List.of(deleted), exchange, allocator, memory);
                    deleted.setRowKind(RowKind.INSERT);
                    while (true)
                        try (var result = ArrowRegularJoinCDataBridge.finish(handle, output, allocator, memory)) {
                            if (result.size() == 0) break;
                            append(result, output, actual);
                        }
                } finally {
                    NativeRegularJoinBridge.destroy(handle);
                }
                actual.sort(Arrays::compareUnsigned);
                assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
                assertThat(memory.reserved).isZero();
            }
        }
    }

    private static void ingest(
            long handle, int side, List<RowData> rows, byte[] exchange, RootAllocator allocator, Memory memory) {
        for (int offset = 0; offset < rows.size(); offset += 7) {
            var chunk = rows.subList(offset, Math.min(offset + 7, rows.size()));
            try (var input = ArrowRowDataBatch.transpose(chunk, INPUT, allocator)
                            .withRowKinds(
                                    chunk.stream().map(RowData::getRowKind).toArray(RowKind[]::new));
                    var envelope = ArrowExchangeBatch.withEnvelope(input, INPUT)) {
                for (var frame : ArrowExchangeCDataBridge.route(exchange, envelope.batch(), allocator, memory))
                    frame.processBoundedRegularJoinNative(handle, side, exchange);
            }
        }
    }
}
