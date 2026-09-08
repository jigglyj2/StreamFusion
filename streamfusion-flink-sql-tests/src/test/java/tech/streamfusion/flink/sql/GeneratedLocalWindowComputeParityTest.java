/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.streamfusion.flink.arrow.ArrowLocalWindowAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowAggregateCDataBridge;
import tech.streamfusion.nativebridge.NativeLocalWindowAggregateBridge;
import tech.streamfusion.nativebridge.NativeWindowAggregateBridge;
import tech.streamfusion.proto.plan.v1.*;

/** SQL is the oracle for local DataFusion partials merged by the retained window kernel. */
class GeneratedLocalWindowComputeParityTest extends NativeComputeParitySupport {
    private static final RowType RAW = RowType.of(new IntType(), new TimestampType(3), new BigIntType());
    private static final RowType PARTIAL = RowType.of(
            new IntType(),
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new BigIntType(false),
            new BigIntType(false));
    private static final RowType RESULT = RowType.of(
            new IntType(),
            new BigIntType(false),
            new BigIntType(false),
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new TimestampType(3),
            new TimestampType(3));

    @ParameterizedTest
    @EnumSource(
            value = WindowKind.class,
            names = {"WINDOW_KIND_TUMBLE", "WINDOW_KIND_HOP", "WINDOW_KIND_CUMULATE"})
    void generatedPartialsMatchFlinkSqlAcrossBatchBoundariesAndBackends(WindowKind kind, @TempDir Path directory)
            throws Exception {
        for (int seed = 0; seed < 3; seed++) {
            var rows = rows(seed);
            var expected = flink(sql(rows, kind), RESULT);
            for (boolean rocks : List.of(false, true)) {
                for (int batchSize : List.of(7, 31)) {
                    var memory = new Memory();
                    long local = NativeLocalWindowAggregateBridge.create(localPlan(kind), memory);
                    long global = 0;
                    var actual = new ArrayList<byte[]>();
                    try (var allocator = new RootAllocator(128L << 20)) {
                        global = rocks
                                ? NativeWindowAggregateBridge.createRocksDb(
                                        globalPlan(kind),
                                        16,
                                        0,
                                        15,
                                        directory.resolve(kind + "-" + seed + "-" + batchSize),
                                        8L << 20,
                                        memory)
                                : NativeWindowAggregateBridge.create(globalPlan(kind), 16, 0, 15, memory);
                        for (int offset = 0; offset < rows.size(); offset += batchSize) {
                            try (var input = ArrowRowDataBatch.transpose(
                                            rows.subList(offset, Math.min(rows.size(), offset + batchSize)),
                                            RAW,
                                            allocator);
                                    var partial = ArrowLocalWindowAggregateCDataBridge.execute(
                                            local, input, false, PARTIAL, allocator, memory);
                                    var result = ArrowWindowAggregateCDataBridge.process(
                                            global, partial, null, false, 0, RESULT, allocator, memory)) {
                                assertThat(result.size()).isZero();
                            }
                        }
                        do {
                            try (var result = ArrowWindowAggregateCDataBridge.advance(
                                    global, false, Long.MAX_VALUE, RESULT, allocator, memory)) {
                                append(result, RESULT, actual);
                            }
                        } while (NativeWindowAggregateBridge.nextEventTimeTimer(global) != Long.MAX_VALUE);
                    } finally {
                        if (global != 0) NativeWindowAggregateBridge.destroy(global);
                        NativeLocalWindowAggregateBridge.destroy(local);
                    }
                    actual.sort(Arrays::compareUnsigned);
                    assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
                    assertThat(memory.reserved).isZero();
                }
            }
        }
    }

    private static List<RowData> rows(int seed) {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        for (int i = 0; i < 67; i++) {
            Long value = i % 11 == 0
                    ? null
                    : i % 11 == 1 ? Long.MAX_VALUE : i % 11 == 2 ? Long.MIN_VALUE : (long) random.nextInt(101) - 50;
            rows.add(GenericRowData.of(
                    i % 13 == 0 ? null : random.nextInt(5),
                    i % 17 == 0 ? null : TimestampData.fromEpochMillis((long) random.nextInt(16001) - 8000),
                    value));
        }
        return rows;
    }

    private static String sql(List<RowData> rows, WindowKind kind) {
        var values = new ArrayList<String>();
        for (var row : rows) {
            String timestamp = row.isNullAt(1)
                    ? "CAST(NULL AS TIMESTAMP(3))"
                    : "TIMESTAMP '"
                            + LocalDateTime.ofInstant(
                                            java.time.Instant.ofEpochMilli(
                                                    row.getTimestamp(1, 3).getMillisecond()),
                                            ZoneOffset.UTC)
                                    .toString()
                                    .replace('T', ' ')
                            + "'";
            values.add("(CAST(" + (row.isNullAt(0) ? "NULL" : row.getInt(0)) + " AS INT), " + timestamp + ", CAST("
                    + (row.isNullAt(2) ? "NULL" : row.getLong(2)) + " AS BIGINT))");
        }
        String function = kind == WindowKind.WINDOW_KIND_TUMBLE
                ? "TUMBLE"
                : kind == WindowKind.WINDOW_KIND_HOP ? "HOP" : "CUMULATE";
        String intervals = kind == WindowKind.WINDOW_KIND_TUMBLE
                ? "INTERVAL '6' SECOND"
                : "INTERVAL '2' SECOND, INTERVAL '6' SECOND";
        return "WITH input(k, ts, v) AS (VALUES " + String.join(",", values) + ") "
                + "SELECT k, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v), window_start, window_end "
                + "FROM TABLE(" + function + "(TABLE input, DESCRIPTOR(ts), " + intervals + ")) "
                + "GROUP BY k, window_start, window_end";
    }

    private static List<AggregateCall> calls() {
        return List.of(
                call(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR, false),
                call(AggregateFunction.AGGREGATE_FUNCTION_COUNT, false),
                call(AggregateFunction.AGGREGATE_FUNCTION_SUM, false),
                call(AggregateFunction.AGGREGATE_FUNCTION_MIN, false),
                call(AggregateFunction.AGGREGATE_FUNCTION_MAX, false),
                call(AggregateFunction.AGGREGATE_FUNCTION_AVG, false));
    }

    private static byte[] localPlan(WindowKind kind) {
        return plan(Operator.newBuilder()
                .setLocalWindowAggregate(LocalWindowAggregate.newBuilder()
                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                        .setInputSchema(schema(RAW))
                        .setOutputSchema(schema(PARTIAL))
                        .addGroupingIndices(0)
                        .addAllAggregateCalls(calls())
                        .setTimeAttributeIndex(1)
                        .setKind(kind)
                        .setSizeMillis(6000)
                        .setSlideOrStepMillis(2000)
                        .setShiftTimeZone("UTC")));
    }

    private static byte[] globalPlan(WindowKind kind) {
        return plan(Operator.newBuilder()
                .setWindowAggregate(WindowAggregate.newBuilder()
                        .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                        .setInputSchema(schema(PARTIAL))
                        .setOutputSchema(schema(RESULT))
                        .addGroupingIndices(0)
                        .addAllAggregateCalls(calls())
                        .setKind(kind)
                        .setSizeMillis(6000)
                        .setSlideOrStepMillis(2000)
                        .setShiftTimeZone("UTC")
                        .setPartialAccumulatorIndex(1)
                        .setPartialWindowStartIndex(2)
                        .setPartialSliceEndIndex(3)
                        .setPartialWindowsAreSlices(true)
                        .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                        .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END)));
    }
}
