/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowTopNCDataBridge;
import tech.streamfusion.nativebridge.NativeTopNBridge;

/** Exact per-arrival changelogs from DataFusion candidate selection versus Flink AppendOnlyTopNFunction. */
class DataFusionAppendTopNChangelogTest {
    @ParameterizedTest
    @CsvSource({
        "false,false,false",
        "false,false,true",
        "false,true,false",
        "false,true,true",
        "true,false,false",
        "true,false,true",
        "true,true,false",
        "true,true,true"
    })
    void generatedNullableCompositeOrderPreservesEveryIntermediateUpdate(
            boolean ascending, boolean rankNumber, boolean before, @TempDir Path directory) throws Exception {
        for (var range : List.of(new int[] {1, 2}, new int[] {1, 10}, new int[] {2, 5}, new int[] {1, 64}))
            for (boolean rocks : List.of(false, true))
                for (int batchSize : List.of(1, 7, 64)) {
                    var fixture = new AppendTopNFlinkFixture(ascending, rankNumber, before, range[0], range[1]);
                    var memory = new SharedChannelStateIO.RoutingMemory();
                    long handle = rocks
                            ? NativeTopNBridge.createRocksDb(
                                    fixture.plan(),
                                    16,
                                    0,
                                    15,
                                    directory.resolve(range[0] + "-" + range[1] + "-" + rocks + "-" + batchSize),
                                    8L << 20,
                                    memory)
                            : NativeTopNBridge.create(fixture.plan(), 16, 0, 15, memory);
                    try (var flink = fixture.oracle(rocks);
                            var allocator = new RootAllocator(64L << 20)) {
                        var serializer = new RowDataSerializer(fixture.input);
                        var rows = rows();
                        for (int start = 0; start < rows.size(); start += batchSize) {
                            var batchRows = rows.subList(start, Math.min(start + batchSize, rows.size()));
                            for (var row : batchRows)
                                flink.processElement(new StreamRecord<>(serializer.toBinaryRow(row)));
                            var expected = new DataOutputSerializer(128);
                            for (var event : flink.getOutput())
                                StageEventBytes.encode(fixture.output, (StreamElement) event, expected);
                            flink.getOutput().clear();
                            try (var batch = ArrowRowDataBatch.transpose(batchRows, fixture.input, allocator);
                                    var output = ArrowTopNCDataBridge.execute(
                                            handle, start, batch, null, fixture.output, allocator, memory)) {
                                var actual = new DataOutputSerializer(128);
                                for (int row = 0; row < output.size(); row++) {
                                    var value = output.rowView(row);
                                    value.setRowKind(output.rowKind(row));
                                    StageEventBytes.row(fixture.output, value, false, 0, actual);
                                }
                                assertThat(actual.getCopyOfBuffer())
                                        .as(
                                                "range=%s..%s rocks=%s batch=%s offset=%s",
                                                range[0], range[1], rocks, batchSize, start)
                                        .containsExactly(expected.getCopyOfBuffer());
                            }
                        }
                    } finally {
                        NativeTopNBridge.destroy(handle);
                    }
                    assertThat(memory.available()).isEqualTo(memory.limit());
                }
    }

    private static List<RowData> rows() {
        var random = new Random(71);
        var rows = new ArrayList<RowData>();
        for (int i = 0; i < 257; i++) {
            Long key = i % 11 == 0 ? null : (long) random.nextInt(7);
            Long score = i % 13 == 0 ? null : (long) random.nextInt(9) - 4;
            Long time = i % 17 == 0 ? Long.MIN_VALUE : i % 19 == 0 ? Long.MAX_VALUE : (long) random.nextInt(3) - 1;
            rows.add(GenericRowData.of(
                    key,
                    score,
                    i % 23 == 0 ? null : TimestampData.fromEpochMillis(time),
                    StringData.fromString("界é-" + i + "x".repeat(i % 31))));
        }
        // Ties extend beyond the cutoff, carrying distinct payloads in arrival order.
        for (int i = 0; i < 80; i++)
            rows.add(GenericRowData.of(100L, 1L, TimestampData.fromEpochMillis(0), StringData.fromString("tie-" + i)));
        rows.add(GenericRowData.of(
                100L, Long.MIN_VALUE, TimestampData.fromEpochMillis(0), StringData.fromString("low")));
        rows.add(GenericRowData.of(
                100L, Long.MAX_VALUE, TimestampData.fromEpochMillis(0), StringData.fromString("high")));
        return rows;
    }
}
