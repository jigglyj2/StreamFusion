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
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowTopNCDataBridge;
import tech.streamfusion.nativebridge.NativeTopNBridge;
import tech.streamfusion.proto.plan.v1.*;

class GeneratedOrderedTopNParityTest extends NativeComputeParitySupport {
    @Test
    void encodedCutoffTiesAndDescendingNullsMatchFlinkSql(@TempDir Path directory) throws Exception {
        var output = RowType.of(new IntType(), new BigIntType(), new BigIntType(), new BigIntType());
        for (boolean ascending : List.of(false, true)) {
            var plan = plan(Operator.newBuilder()
                    .setTopN(TopN.newBuilder()
                            .setInputSchema(schema(INPUT))
                            .setOutputSchema(schema(output))
                            .addPartitionKeyIndices(0)
                            .addSortKeyIndices(2)
                            .addSortAscending(ascending)
                            .addSortNullsLast(false)
                            .setRankStart(2)
                            .setRankEnd(5)
                            .setOutputRankNumber(true)
                            .setRankType(TopNRankType.TOP_N_RANK_TYPE_RANK)
                            .setStrategy(TopNStrategy.TOP_N_STRATEGY_APPEND_FAST)
                            .setBoundedFinalOutput(true)
                            .setPhysicalInputSemantics(true)));
            for (int seed = 0; seed < 2; seed++) {
                var rows = generated(seed);
                var expected = flink(
                        "SELECT k,o,v,r FROM (SELECT k,o,v,RANK() OVER (PARTITION BY k ORDER BY v "
                                + (ascending ? "ASC" : "DESC") + " NULLS FIRST) AS r FROM " + relation(rows, "t")
                                + ") WHERE r BETWEEN 2 AND 5",
                        output);
                for (boolean rocks : List.of(false, true)) {
                    var memory = new Memory();
                    long handle = rocks
                            ? NativeTopNBridge.createRocksDb(
                                    plan,
                                    16,
                                    0,
                                    15,
                                    directory.resolve(ascending + "-" + seed + "-" + rocks),
                                    8L << 20,
                                    memory)
                            : NativeTopNBridge.create(plan, 16, 0, 15, memory);
                    var actual = new ArrayList<byte[]>();
                    try (var allocator = new RootAllocator(128L << 20)) {
                        for (int offset = 0; offset < rows.size(); offset += 3)
                            try (var input = ArrowRowDataBatch.transpose(
                                            rows.subList(offset, Math.min(offset + 3, rows.size())), INPUT, allocator);
                                    var result = ArrowTopNCDataBridge.execute(
                                            handle, 0, input, null, output, allocator, memory)) {
                                assertThat(result.size()).isZero();
                            }
                        while (true)
                            try (var result = ArrowTopNCDataBridge.finish(handle, output, allocator, memory)) {
                                if (result.size() == 0) break;
                                append(result, output, actual);
                            }
                    } finally {
                        NativeTopNBridge.destroy(handle);
                    }
                    actual.sort(Arrays::compareUnsigned);
                    assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
                    assertThat(memory.reserved).isZero();
                }
            }
        }
    }
}
