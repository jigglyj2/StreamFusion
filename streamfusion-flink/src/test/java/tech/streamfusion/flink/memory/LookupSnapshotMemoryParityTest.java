/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowLookupSnapshotBindings;
import tech.streamfusion.flink.arrow.ArrowNativePlanBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LookupJoin;
import tech.streamfusion.proto.plan.v1.LookupJoinKind;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

@SuppressWarnings("deprecation")
class LookupSnapshotMemoryParityTest {
    @TempDir
    Path directory;

    private static final RowType PROBE = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {new BigIntType(), new VarCharType()},
            new String[] {"key", "probe"});
    private static final RowType SIDE = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {new BigIntType(), new VarCharType()},
            new String[] {"key", "value"});
    private static final RowType OUTPUT =
            RowType.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType());

    @Test
    void generatedCsvLookupChangelogsCrossCStreamWithOriginalFlinkResultsAndMetadata() throws Exception {
        for (int seed : new int[] {3, 17, 91}) {
            Path path = directory.resolve("side-" + seed + ".csv");
            StringBuilder csv = new StringBuilder();
            for (int i = 0; i < 73; i++) {
                csv.append(i % 11 == 0 ? "" : Integer.toString((i * seed) % 7 - 3));
                csv.append(',').append(i % 13 == 0 ? "" : "é-" + seed + "-" + i).append('\n');
            }
            Files.writeString(path, csv.toString());
            CsvTableSource table = table(path);
            var oracle = (CsvTableSource.CsvLookupFunction) table.getLookupFunction(new String[] {"key"});
            oracle.open(new FunctionContext(null));
            try {
                List<RowData> probes = new ArrayList<>();
                List<BinaryRowData> expected = new ArrayList<>();
                RowDataSerializer serializer = new RowDataSerializer(OUTPUT);
                for (int i = 0; i < 37; i++) {
                    Long key = i % 11 == 0 ? null : (long) (i % 9 - 4);
                    GenericRowData probe = GenericRowData.of(key, StringData.fromString("probe-" + i));
                    probe.setRowKind(RowKind.values()[i % 4]);
                    probes.add(probe);
                    oracle.setCollector(new Collector<Row>() {
                        @Override
                        public void collect(Row side) {
                            GenericRowData joined = GenericRowData.of(
                                    key,
                                    probe.getString(1),
                                    side.getField(0),
                                    side.getField(1) == null ? null : StringData.fromString((String) side.getField(1)));
                            joined.setRowKind(probe.getRowKind());
                            expected.add(serializer.toBinaryRow(joined).copy());
                        }

                        @Override
                        public void close() {}
                    });
                    // Flink's generated lookup runner filters SQL-null equality probes.
                    if (key != null) oracle.eval(key);
                }
                for (int chunk : new int[] {1, 7, 128}) {
                    MemoryManager manager = MemoryManager.create(64L << 20, 32 * 1024);
                    try (FlinkManagedMemory memory = new FlinkManagedMemory(manager, 32L << 20, "lookup")) {
                        try (NativeExecutionContext nativePlan = open(table, memory, chunk);
                                ArrowRowDataBatch probe =
                                        ArrowRowDataBatch.transpose(probes, PROBE, memory.allocator())) {
                            RowKind[] kinds = new RowKind[probes.size()];
                            boolean[] present = new boolean[probes.size()];
                            long[] times = new long[probes.size()];
                            for (int i = 0; i < kinds.length; i++) {
                                kinds[i] = probes.get(i).getRowKind();
                                present[i] = i % 3 != 0;
                                times[i] = i - 100L;
                            }
                            probe.withEnvelope(kinds, present, times);
                            ArrowNativePlanBridge edge =
                                    new ArrowNativePlanBridge(nativePlan, OUTPUT, memory.allocator());
                            for (int invocation = 0; invocation < 3; invocation++) {
                                List<BinaryRowData> actual = new ArrayList<>();
                                try (var stream = edge.executeStream(List.of(probe))) {
                                    ArrowRowDataBatch next;
                                    while ((next = stream.next()) != null) {
                                        try (ArrowRowDataBatch output = next) {
                                            for (int row = 0; row < output.size(); row++) {
                                                RowData value = output.rowView(row);
                                                // Match ArrowBatchToRowDataOperator: changelog kind lives in the
                                                // envelope.
                                                value.setRowKind(output.rowKind(row));
                                                actual.add(serializer
                                                        .toBinaryRow(value)
                                                        .copy());
                                                int index = Integer.parseInt(value.getString(1)
                                                        .toString()
                                                        .substring(6));
                                                assertThat(output.rowKind(row)).isEqualTo(kinds[index]);
                                                assertThat(output.hasTimestamp(row))
                                                        .isEqualTo(present[index]);
                                                if (present[index])
                                                    assertThat(output.timestamp(row))
                                                            .isEqualTo(times[index]);
                                            }
                                        }
                                    }
                                }
                                assertThat(actual).containsExactlyElementsOf(expected);
                                assertThat(nativePlan.metricSnapshot())
                                        .containsExactly(
                                                2,
                                                probes.size() * (invocation + 1L),
                                                expected.size() * (invocation + 1L),
                                                1,
                                                0,
                                                probes.size() * (invocation + 1L));
                            }
                        }
                        assertThat(memory.reserved()).isZero();
                        assertThat(memory.allocator().getAllocatedMemory()).isZero();
                    } finally {
                        assertThat(manager.verifyEmpty()).isTrue();
                        manager.shutdown();
                    }
                }
            } finally {
                oracle.close();
            }
        }
    }

    @Test
    void retainedJavaSnapshotPayloadIsChargedOnceAndReloadedAtNextTaskOpen() throws Exception {
        Path path = directory.resolve("wide.csv");
        CsvTableSource table = table(path);
        long[] nativeBytes = new long[2];
        long[] arrowBytes = new long[2];
        MemoryManager manager = MemoryManager.create(64L << 20, 32 * 1024);
        try (FlinkManagedMemory memory = new FlinkManagedMemory(manager, 32L << 20, "lookup")) {
            for (int run = 0; run < 2; run++) {
                Files.writeString(path, ("1," + "a".repeat(run == 0 ? 1 : 65536) + "\n").repeat(17));
                try (NativeExecutionContext nativePlan = open(table, memory, 128)) {
                    nativeBytes[run] = memory.reserved();
                    arrowBytes[run] = memory.allocator().getAllocatedMemory();
                    assertThat(arrowBytes[run]).isPositive();
                }
                assertThat(memory.reserved()).isZero();
            }
            assertThat(arrowBytes[1]).isGreaterThan(arrowBytes[0]);
            assertThat(nativeBytes[1] - nativeBytes[0]).isEqualTo(arrowBytes[1] - arrowBytes[0]);
        } finally {
            assertThat(manager.verifyEmpty()).isTrue();
            manager.shutdown();
        }
    }

    @Test
    void sourceChangesWaitForTaskReopenAndRetainedOutputsOutliveTheCache() throws Exception {
        Path path = directory.resolve("reload.csv");
        Files.writeString(path, "1,old\n");
        CsvTableSource table = table(path);
        MemoryManager manager = MemoryManager.create(64L << 20, 32 * 1024);
        try (FlinkManagedMemory memory = new FlinkManagedMemory(manager, 32L << 20, "lookup")) {
            for (String expected : List.of("old", "new")) {
                ArrowRowDataBatch retained;
                try (NativeExecutionContext nativePlan = open(table, memory, 7);
                        ArrowRowDataBatch probe = ArrowRowDataBatch.transpose(
                                List.of(GenericRowData.of(1L, StringData.fromString("probe"))),
                                PROBE,
                                memory.allocator())) {
                    Files.writeString(path, "1,new\n");
                    try (var stream = new ArrowNativePlanBridge(nativePlan, OUTPUT, memory.allocator())
                            .executeStream(List.of(probe))) {
                        retained = stream.next();
                        assertThat(retained).isNotNull();
                        assertThat(stream.next()).isNull();
                    }
                }
                try (ArrowRowDataBatch output = retained) {
                    assertThat(memory.reserved()).isPositive();
                    assertThat(output.rowView(0).getString(3).toString()).isEqualTo(expected);
                }
                assertThat(memory.reserved()).isZero();
            }
        } finally {
            assertThat(manager.verifyEmpty()).isTrue();
            manager.shutdown();
        }
    }

    @Test
    void parseFailureAndManagedMemoryDenialReleaseEverySourceAndNativeOwner() throws Exception {
        for (boolean denied : new boolean[] {false, true}) {
            Path path = directory.resolve("failure.csv");
            Files.writeString(path, denied ? ("1," + "x".repeat(65536) + "\n").repeat(64) : "1,ok\nbad,value\n");
            MemoryManager manager = MemoryManager.create(8L << 20, 32 * 1024);
            try (FlinkManagedMemory memory = new FlinkManagedMemory(manager, 1L << 20, "lookup")) {
                assertThatThrownBy(() -> open(table(path), memory, 1))
                        .hasMessageContaining(denied ? "memory" : "ParseException");
                assertThat(memory.reserved()).isZero();
                assertThat(memory.allocator().getAllocatedMemory()).isZero();
            } finally {
                assertThat(manager.verifyEmpty()).isTrue();
                manager.shutdown();
            }
        }
    }

    private NativeExecutionContext open(CsvTableSource table, FlinkManagedMemory memory, int chunk) throws Exception {
        return ArrowLookupSnapshotBindings.create(
                plan(),
                memory,
                null,
                null,
                false,
                Map.of(2L, CsvLookupSnapshotSource.from(table)),
                memory.allocator(),
                getClass().getClassLoader(),
                chunk);
    }

    private static CsvTableSource table(Path path) {
        return CsvTableSource.builder()
                .path(path.toUri().toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .emptyColumnAsNull()
                .build();
    }

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields())
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        return schema.build();
    }

    private static byte[] plan() {
        return NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(2)
                        .setLookupJoin(LookupJoin.newBuilder()
                                .setKind(LookupJoinKind.LOOKUP_JOIN_KIND_INNER)
                                .setInputSchema(schema(PROBE))
                                .setSideSchema(schema(SIDE))
                                .setOutputSchema(schema(OUTPUT))
                                .addProbeKeys(0)
                                .addSideKeys(0)
                                .setInput(Operator.newBuilder().setPlanNodeId(1).setInput(Input.getDefaultInstance()))))
                .build()
                .toByteArray();
    }
}
