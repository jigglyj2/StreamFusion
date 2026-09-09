/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;
import tech.streamfusion.flink.join.NativeLookupSources;
import tech.streamfusion.flink.join.StreamFusionLookupJoinPlan;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;

/** Uses Flink's actual lookup code generator, CSV function and ProcessOperator as the oracle. */
@SuppressWarnings("deprecation")
class LookupRegionRuntimeTest {
    @TempDir
    Path directory;

    static final RowType PROBE = RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
    static final RowType SIDE = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)
            },
            new String[] {"key", "value"});
    static final RowType OUTPUT = RowType.of(
            new BigIntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BigIntType(),
            new VarCharType(VarCharType.MAX_LENGTH));
    static final long LOOKUP = (1L << 32) | 13;

    @Test
    void generatedChangelogsTimestampsAndControlsMatchOriginalFlinkLookup() throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var random = new Random(seed);
            var file = directory.resolve("side-" + seed + ".csv");
            var csv = new StringBuilder();
            for (int i = 0; i < 73; i++)
                csv.append(i % 11 == 0 ? "" : Integer.toString(random.nextInt(9) - 4))
                        .append(',')
                        .append(i % 13 == 0 ? "" : "é-" + i)
                        .append('\n');
            Files.writeString(file, csv);
            var table = table(file);
            FlinkManagedMemory memory;
            try (var allocator = new RootAllocator(64L << 20);
                    var nativeHarness = new NativeRegionTestHarness(factory(table), List.of(OUTPUT));
                    var flink = flink(table, seed != 19)) {
                nativeHarness.open();
                memory = memory(nativeHarness);
                var metrics = new LookupMetricOracle(
                        flink.getOperator().getMetricGroup(), nativeHarness.stageMetrics(LOOKUP));
                metrics.compare();
                var expectedRows = new ArrayList<RowData>();
                var expectedTimes = new ArrayList<Long>();
                for (int start = 0; start < 113; start += 17) {
                    var rows = new ArrayList<RowData>();
                    int count = Math.min(17, 113 - start);
                    var kinds = new RowKind[count];
                    var present = new boolean[count];
                    var times = new long[count];
                    for (int i = 0; i < count; i++) {
                        var row = GenericRowData.of(
                                (start + i) % 7 == 0 ? null : (long) (random.nextInt(13) - 6),
                                StringData.fromString("probe-" + (start + i)));
                        kinds[i] = RowKind.values()[(start + i) % 4];
                        row.setRowKind(kinds[i]);
                        rows.add(row);
                        present[i] = i % 3 != 0;
                        times[i] = start + i - 90L;
                        flink.processElement(present[i] ? new StreamRecord<>(row, times[i]) : new StreamRecord<>(row));
                    }
                    int previousOutput = expectedRows.size();
                    for (Object event : flink.getOutput()) {
                        var record = (StreamRecord<?>) event;
                        expectedRows.add((RowData) record.getValue());
                        expectedTimes.add(record.hasTimestamp() ? record.getTimestamp() : null);
                    }
                    flink.getOutput().clear();
                    try (var batch =
                            ArrowRowDataBatch.transpose(rows, PROBE, allocator).withEnvelope(kinds, present, times)) {
                        nativeHarness.accept(0, batch);
                    }
                    assertThat(bytes(nativeHarness.rows)).containsExactly(bytes(expectedRows));
                    assertThat(nativeHarness.outputTimes.get(0)).containsExactlyElementsOf(expectedTimes);
                    var io = nativeHarness.stageMetrics(LOOKUP).getIOMetricGroup();
                    assertThat(io.getNumRecordsInCounter().getCount()).isEqualTo(start + count);
                    assertThat(io.getNumRecordsOutCounter().getCount()).isEqualTo(expectedRows.size());
                    metrics.records(count, expectedRows.size() - previousOutput);
                }
                for (long time : List.of(100L, 200L)) {
                    flink.processWatermark(new Watermark(time));
                    nativeHarness.processWatermark(0, new Watermark(time));
                    flink.processWatermarkStatus(WatermarkStatus.IDLE);
                    nativeHarness.processWatermarkStatus(0, WatermarkStatus.IDLE);
                    flink.processWatermarkStatus(WatermarkStatus.ACTIVE);
                    nativeHarness.processWatermarkStatus(0, WatermarkStatus.ACTIVE);
                    metrics.watermark(time);
                }
                assertThat(flink.getOutput()).containsExactlyElementsOf(nativeHarness.controls);
                nativeHarness.prepareSnapshotPreBarrier(7);
                nativeHarness.snapshot(7, 200);
                nativeHarness.notifyOfCompletedCheckpoint(7);
                metrics.compare();
                nativeHarness.end(0);
                try (var empty = ArrowRowDataBatch.empty(PROBE, allocator)) {
                    assertThatThrownBy(() -> nativeHarness.accept(0, empty)).hasMessageContaining("ended");
                }
            }
            assertThat(memory.reserved()).isZero();
        }
    }

    static CsvTableSource table(Path file) {
        return CsvTableSource.builder()
                .path(file.toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .build();
    }

    static byte[] plan() {
        return StreamFusionNativeRegionTranslator.identifyStage(
                StreamFusionLookupJoinPlan.createStagePlan(PROBE, SIDE, OUTPUT, new int[] {0}, new int[] {0}), 13);
    }

    static StreamFusionNativeRegionOperatorFactory factory(CsvTableSource table) throws Exception {
        return new StreamFusionNativeRegionOperatorFactory(List.of(PROBE), OUTPUT, plan())
                .withLookupSources(new NativeLookupSources(Map.of(LOOKUP, CsvLookupSnapshotSource.from(table))));
    }

    static OneInputStreamOperatorTestHarness<RowData, RowData> flink(CsvTableSource table, boolean objectReuse)
            throws Exception {
        return flink(table, objectReuse, null);
    }

    static OneInputStreamOperatorTestHarness<RowData, RowData> flink(
            CsvTableSource table,
            boolean objectReuse,
            org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot)
            throws Exception {
        var loader = LookupRegionRuntimeTest.class.getClassLoader();
        var config = new Configuration();
        var environment = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        var fetch = LookupJoinCodeGenerator.generateSyncLookupFunction(
                config,
                loader,
                environment.getCatalogManager().getDataTypeFactory(),
                PROBE,
                SIDE,
                OUTPUT,
                List.of(new FunctionCallUtil.FieldRef(0)),
                table.getLookupFunction(new String[] {"key"}),
                "csv_lookup",
                objectReuse);
        var collector = LookupJoinCodeGenerator.generateCollector(
                new CodeGeneratorContext(config, loader),
                PROBE,
                SIDE,
                OUTPUT,
                scala.Option.empty(),
                scala.Option.empty(),
                true);
        var filter = FilterCodeGenerator.generateFilterCondition(config, loader, null, PROBE);
        var runner = new LookupJoinRunner(fetch, collector, filter, false, SIDE.getFieldCount());
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(new ProcessOperator<>(runner));
        harness.setup(new RowDataSerializer(OUTPUT));
        if (objectReuse) harness.getExecutionConfig().enableObjectReuse();
        if (snapshot != null) harness.initializeState(snapshot);
        harness.open();
        return harness;
    }

    static byte[] bytes(List<RowData> rows) throws Exception {
        var out = new DataOutputSerializer(128);
        var serializer = new RowDataSerializer(OUTPUT);
        for (var row : rows) serializer.serialize(row, out);
        return out.getCopyOfBuffer();
    }

    static FlinkManagedMemory memory(NativeRegionTestHarness harness) throws Exception {
        var field = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("memory");
        field.setAccessible(true);
        return (FlinkManagedMemory) ((StreamFusionTaskMemory) field.get(harness.region())).nativeMemoryManager();
    }
}
