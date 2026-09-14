/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.state.rocksdb.RocksDBWriteBatchWrapper;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

class RocksDbWriteBatchTraceTest {
    @org.junit.jupiter.api.io.TempDir
    Path temporary;

    @Test
    void actualFlinkBulkWriterMatchesTheSharedNativeFlushFixture() throws Exception {
        RocksDB.loadLibrary();
        var lines = new java.util.ArrayList<String>();
        for (long limit : new long[] {0, 1, 50, 4096, 2097152, Long.MAX_VALUE}) {
            long[] hash = {0xcbf29ce484222325L};
            int[] batches = {0};
            var db = mock(RocksDB.class);
            doAnswer(invocation -> {
                        WriteBatch batch = invocation.getArgument(1);
                        for (byte value : batch.data()) hash[0] = (hash[0] ^ (value & 255)) * 0x100000001b3L;
                        batches[0]++;
                        return null;
                    })
                    .when(db)
                    .write(any(WriteOptions.class), any(WriteBatch.class));
            try (var actual = RocksDB.open(temporary.resolve("db-" + limit).toString());
                    var handle = actual.getDefaultColumnFamily();
                    var wrapper = new RocksDBWriteBatchWrapper(db, limit)) {
                for (int row = 0; row < 1103; row++) {
                    byte[] key = new byte[4 + (row % 11 == 0 ? 128 : 8)];
                    for (int index = 4; index < key.length; index++) key[index] = (byte) ((row + index - 4) % 251);
                    if (row % 7 == 0) wrapper.remove(handle, key);
                    else {
                        byte[] value = new byte[row % 113 == 0 ? 4096 : row * 37 % 260];
                        for (int index = 0; index < value.length; index++) value[index] = (byte) ((row + index) % 251);
                        wrapper.put(handle, key, value);
                    }
                }
            }
            lines.add(limit + " " + batches[0] + " " + Long.toUnsignedString(hash[0], 16));
        }
        Files.write(Path.of("target/flink-write-batch-traces.txt"), lines);
        assertThat(lines)
                .containsExactlyElementsOf(Files.readAllLines(
                        Path.of("../streamfusion-state-rocksdb/tests/fixtures/flink-write-batch-traces.txt")));
    }
}
