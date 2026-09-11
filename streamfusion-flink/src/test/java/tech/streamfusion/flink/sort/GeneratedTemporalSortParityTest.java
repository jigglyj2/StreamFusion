/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.sort.StreamFusionArrowTemporalSortOperatorTest.PARITY_ROW_TYPE;
import static tech.streamfusion.flink.sort.StreamFusionArrowTemporalSortOperatorTest.SORT_SPEC;
import static tech.streamfusion.flink.sort.StreamFusionArrowTemporalSortOperatorTest.harness;
import static tech.streamfusion.flink.sort.StreamFusionArrowTemporalSortOperatorTest.process;
import static tech.streamfusion.flink.sort.StreamFusionArrowTemporalSortOperatorTest.row;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.operators.sort.ProcTimeSortOperator;
import org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/** Compares the upstream Flink operator with indexed native rows, including restored arrival ties. */
class GeneratedTemporalSortParityTest {
    @Test
    void generatedChangelogsMatchFlinkBytesAfterCrossBackendRestoreAndLateRows() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            List<GenericRowData> input = input(seed);
            byte[] expected = flink(input, false);
            for (boolean sourceRocks : new boolean[] {false, true}) {
                try (RootAllocator allocator = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState snapshot;
                    int batchSize = new int[] {1, 17, 128, 257}[seed];
                    try (var source = harness(false, null, sourceRocks, 64L << 20)) {
                        append(source, allocator, input.subList(0, 256), batchSize);
                        snapshot = source.snapshotWithLocalState(
                                        1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                    }
                    try (var restored = harness(false, snapshot, !sourceRocks, 64L << 20)) {
                        append(restored, allocator, input.subList(256, input.size()), batchSize);
                        restored.processWatermark(new Watermark(3000));
                        process(restored, allocator, row(1000, 0, "late", RowKind.DELETE));
                        process(restored, allocator, row(4000, 0, "future", RowKind.INSERT));
                        restored.processWatermark(new Watermark(4000));
                        assertThat(restored.bytes.getCopyOfBuffer())
                                .as("seed %s, source RocksDB %s", seed, sourceRocks)
                                .isEqualTo(expected);
                        assertIo(restored, input.size() - 256 + 2, input.size() + 1);
                    }
                }
            }
        }
    }

    @Test
    void generatedProcessingTimersMatchFlinkBytesAcrossRestoreAndRepeatedCallbacks() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            List<GenericRowData> input = input(seed);
            byte[] expected = flink(input, true);
            for (boolean sourceRocks : new boolean[] {false, true}) {
                try (RootAllocator allocator = new RootAllocator(64L << 20)) {
                    OperatorSubtaskState snapshot;
                    int batchSize = new int[] {1, 17, 128, 257}[seed];
                    try (var source = harness(true, null, sourceRocks, 64L << 20)) {
                        source.setProcessingTime(40);
                        append(source, allocator, input.subList(0, 256), batchSize);
                        snapshot = source.snapshotWithLocalState(
                                        1, 1, SavepointType.savepoint(SavepointFormatType.CANONICAL))
                                .getJobManagerOwnedState();
                    }
                    try (var restored = harness(true, snapshot, !sourceRocks, 64L << 20)) {
                        restored.setProcessingTime(40);
                        append(restored, allocator, input.subList(256, input.size()), batchSize);
                        restored.setProcessingTime(41);
                        process(restored, allocator, row(4000, 0, "future", RowKind.INSERT));
                        restored.setProcessingTime(42);
                        assertThat(restored.bytes.getCopyOfBuffer())
                                .as("processing-time seed %s, source RocksDB %s", seed, sourceRocks)
                                .isEqualTo(expected);
                        assertIo(restored, input.size() - 256 + 1, input.size() + 1);
                    }
                }
            }
        }
    }

    private static void assertIo(StreamFusionArrowTemporalSortOperatorTest.Harness harness, long inputs, long outputs) {
        var io = harness.getOperator().getMetricGroup().getIOMetricGroup();
        assertThat(io.getNumRecordsInCounter().getCount()).isEqualTo(inputs);
        assertThat(io.getNumRecordsOutCounter().getCount()).isEqualTo(outputs);
    }

    private static void append(
            StreamFusionArrowTemporalSortOperatorTest.Harness harness,
            RootAllocator allocator,
            List<GenericRowData> rows,
            int batchSize)
            throws Exception {
        for (int start = 0; start < rows.size(); start += batchSize) {
            process(
                    harness,
                    allocator,
                    rows.subList(start, Math.min(start + batchSize, rows.size()))
                            .toArray(new GenericRowData[0]));
        }
    }

    private static List<GenericRowData> input(int seed) {
        Random random = new Random(9217 + seed);
        List<GenericRowData> rows = new ArrayList<>();
        for (int index = 0; index < (seed == 3 ? 2049 : 512); index++) {
            GenericRowData row = row(
                    1000L * (1 + random.nextInt(3)),
                    random.nextInt(7),
                    "é-" + index + (seed == 3 ? "é".repeat(128) : ""),
                    RowKind.values()[index % 4]);
            if (index % 11 == 0) {
                row.setField(16, null);
                row.setField(17, null);
                row.setField(18, null);
            }
            rows.add(row);
        }
        return rows;
    }

    private byte[] flink(List<GenericRowData> input, boolean processingTime) throws Exception {
        var comparator = new SortCodeGenerator(
                        new Configuration(),
                        getClass().getClassLoader(),
                        PARITY_ROW_TYPE,
                        processingTime
                                ? SortSpec.builder().addField(1, true, true).build()
                                : SORT_SPEC)
                .generateRecordComparator("GeneratedTemporalParityComparator");
        var operator = processingTime
                ? new ProcTimeSortOperator(InternalTypeInfo.of(PARITY_ROW_TYPE), comparator)
                : new RowTimeSortOperator(InternalTypeInfo.of(PARITY_ROW_TYPE), 0, comparator);
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                operator,
                EmptyRowDataKeySelector.INSTANCE,
                EmptyRowDataKeySelector.INSTANCE.getProducedType(),
                1,
                1,
                0)) {
            RowDataSerializer serializer = new RowDataSerializer(PARITY_ROW_TYPE);
            harness.setup(serializer);
            harness.open();
            if (processingTime) {
                harness.setProcessingTime(40);
            }
            for (GenericRowData row : input) {
                // Flink's temporal operator reads the compact millisecond field through getLong.
                harness.processElement(
                        new StreamRecord<>(serializer.toBinaryRow(row).copy()));
            }
            if (processingTime) {
                harness.setProcessingTime(41);
            } else {
                harness.processWatermark(new Watermark(3000));
                harness.processElement(new StreamRecord<>(serializer
                        .toBinaryRow(row(1000, 0, "late", RowKind.DELETE))
                        .copy()));
            }
            harness.processElement(new StreamRecord<>(serializer
                    .toBinaryRow(row(4000, 0, "future", RowKind.INSERT))
                    .copy()));
            if (processingTime) {
                harness.setProcessingTime(42);
            } else {
                harness.processWatermark(new Watermark(4000));
            }
            DataOutputSerializer bytes = new DataOutputSerializer(1024);
            for (RowData row : harness.extractOutputValues()) {
                serializer.serialize(row, bytes);
            }
            return bytes.getCopyOfBuffer();
        }
    }
}
