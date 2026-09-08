/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.table.runtime.util.collections.binary.WindowBytesMultiMap;
import org.junit.jupiter.api.Test;

/** Upstream page geometry oracle for native flush-capacity calculation, including table growth. */
class LocalWindowBufferLayoutOracleTest {
    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void capturesFlushBoundariesForFixedAndVariableRowsAndReusedGrownBuckets() throws Exception {
        var cases = java.nio.file.Files.readAllLines(java.nio.file.Path.of(
                "..",
                "streamfusion-native",
                "src",
                "planner",
                "operators",
                "local_window_aggregate",
                "buffer_layout_cases.csv"));
        for (var line : cases.subList(1, cases.size())) {
            int[] spec = java.util.Arrays.stream(line.split(","))
                    .mapToInt(Integer::parseInt)
                    .toArray();
            var memory = MemoryManagerBuilder.newBuilder()
                    .setMemorySize((long) spec[0] << 20)
                    .build();
            int keyArity = spec[2] == 0 ? 1 : 2;
            int inputArity = spec[3] == 0 ? 2 : 3;
            var table = new WindowBytesMultiMap(
                    this,
                    memory,
                    (long) spec[0] << 20,
                    (PagedTypeSerializer) new BinaryRowDataSerializer(keyArity),
                    inputArity);
            var key = new BinaryRowData(keyArity);
            var writer = new BinaryRowWriter(key);
            var keyPayload = new byte[spec[2]];
            var input = new BinaryRowData(inputArity);
            var inputWriter = new BinaryRowWriter(input);
            inputWriter.writeLong(0, 1);
            inputWriter.writeLong(1, 1000);
            if (inputArity == 3) inputWriter.writeBinary(2, new byte[spec[3]]);
            inputWriter.complete();
            var windowKey = new WindowKey(2000, key);
            try {
                var counts = new int[2];
                for (int pass = 0; pass < 2; pass++) {
                    int row = 0;
                    while (true) {
                        writer.reset();
                        writer.writeLong(0, row % spec[1]);
                        if (keyArity == 2) writer.writeBinary(1, keyPayload);
                        writer.complete();
                        try {
                            table.append(table.lookup(windowKey), input);
                            row++;
                            assertThat(row).isLessThan(1_000_000);
                        } catch (EOFException full) {
                            counts[pass] = row;
                            break;
                        }
                    }
                    table.reset();
                }
                assertThat(key.getSizeInBytes()).isEqualTo(spec[5]);
                assertThat(input.getSizeInBytes()).isEqualTo(spec[7]);
                assertThat(counts).as("upstream Flink fixture %s", line).containsExactly(spec[8], spec[8]);
            } finally {
                table.free();
                assertThat(memory.verifyEmpty()).isTrue();
                memory.shutdown();
            }
        }
    }
}
