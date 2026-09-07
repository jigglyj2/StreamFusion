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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowGroupAggregateCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.nativebridge.NativeGroupAggregateBridge;
import tech.streamfusion.proto.plan.v1.*;

class GeneratedDataFusionAggregateParityTest extends NativeComputeParitySupport {
    @Test
    void boundedAggregateKernelsMatchFlinkSqlAcrossBatchesAndBackends(@TempDir Path directory) throws Exception {
        var output = RowType.of(
                new IntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType());
        var aggregate = GroupAggregate.newBuilder()
                .setInputSchema(schema(INPUT))
                .setOutputSchema(schema(output))
                .addGroupingIndices(0)
                .setBoundedFinalOutput(true);
        for (var fn : List.of(
                AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR,
                AggregateFunction.AGGREGATE_FUNCTION_COUNT,
                AggregateFunction.AGGREGATE_FUNCTION_SUM,
                AggregateFunction.AGGREGATE_FUNCTION_MIN,
                AggregateFunction.AGGREGATE_FUNCTION_MAX,
                AggregateFunction.AGGREGATE_FUNCTION_AVG)) aggregate.addAggregateCalls(call(fn, false));
        byte[] plan = plan(Operator.newBuilder().setGroupAggregate(aggregate));
        for (int seed = 0; seed < 3; seed++) {
            var rows = generated(seed);
            var expected = flink(
                    "SELECT k, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM " + relation(rows, "t")
                            + " GROUP BY k",
                    output);
            for (boolean rocks : List.of(false, true)) {
                var memory = new Memory();
                long handle = rocks
                        ? NativeGroupAggregateBridge.createRocksDb(
                                plan, 16, 0, 15, directory.resolve("aggregate-" + seed), 8L << 20, memory)
                        : NativeGroupAggregateBridge.create(plan, 16, 0, 15, memory);
                var actual = new ArrayList<byte[]>();
                try (var allocator = new RootAllocator(128L << 20)) {
                    for (int offset = 0; offset < rows.size(); offset += 7) {
                        try (var input = ArrowRowDataBatch.transpose(
                                        rows.subList(offset, Math.min(offset + 7, rows.size())), INPUT, allocator);
                                var result = ArrowGroupAggregateCDataBridge.execute(
                                        handle, input, null, false, output, allocator, memory)) {
                            assertThat(result.size()).isZero();
                        }
                    }
                    while (true) {
                        try (var result =
                                ArrowGroupAggregateCDataBridge.finishBundle(handle, output, allocator, memory)) {
                            if (result.size() == 0) break;
                            append(result, output, actual);
                        }
                    }
                } finally {
                    NativeGroupAggregateBridge.destroy(handle);
                }
                actual.sort(Arrays::compareUnsigned);
                assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
                assertThat(memory.reserved).isZero();
            }
        }
    }
}
