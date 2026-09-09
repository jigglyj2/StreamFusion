/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.streamfusion.flink.operator.LookupRegionRuntimeTest.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.join.NativeLookupSources;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.NativeRegionInputReference;

class LookupRegionCompositionTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void generatedFlinkCalcLookupCalcMatchesOneNativeTreeAndSharedLookupExit(boolean shared) throws Exception {
        var file = directory.resolve("side.csv");
        Files.writeString(file, "1,one\n1,duplicate\n3,three\n,empty\n");
        var table = table(file);
        var before = new Calc(PROBE, List.of(0, 1), 11);
        var after = new Calc(OUTPUT, List.of(2, 3, 0, 1), 14);
        var fragments = List.of(before.plan, plan(), after.plan);
        byte[] tree = shared
                ? NativeRegionPlanComposer.compose(
                        1,
                        fragments,
                        List.of(
                                List.of(NativeRegionInputReference.newBuilder()
                                        .setExternalInput(0)
                                        .build()),
                                List.of(NativeRegionInputReference.newBuilder()
                                        .setStageId((1L << 32) | 11)
                                        .build()),
                                List.of(NativeRegionInputReference.newBuilder()
                                        .setStageId(LOOKUP)
                                        .build())),
                        List.of(LOOKUP, (1L << 32) | 14))
                : StreamFusionNativeRegionTranslator.compose(fragments);
        var outputs = shared ? List.of(OUTPUT, OUTPUT) : List.of(OUTPUT);
        var factory = shared
                ? StreamFusionNativeRegionOperatorFactory.shared(
                        List.of(PROBE),
                        outputs,
                        tree,
                        List.of(),
                        List.of(NativeExchangePlanSerializer.singleton(PROBE)),
                        NativeLocalWindowResources.NONE)
                : new StreamFusionNativeRegionOperatorFactory(List.of(PROBE), OUTPUT, tree);
        factory.withLookupSources(new NativeLookupSources(Map.of(LOOKUP, CsvLookupSnapshotSource.from(table))));
        try (var allocator = new RootAllocator(64L << 20);
                var nativeHarness = new NativeRegionTestHarness(factory, outputs);
                var first = before.flink();
                var lookup = flink(table, true);
                var last = after.flink()) {
            nativeHarness.open();
            var expectedLookup = new ArrayList<RowData>();
            var expectedOutput = new ArrayList<RowData>();
            long probeCount = 0;
            for (int batchIndex = 0; batchIndex < 4; batchIndex++) {
                var rows = new ArrayList<RowData>();
                for (int i = 0; i < 19; i++) {
                    var row = GenericRowData.of(i % 7 == 0 ? null : (long) (i % 5), StringData.fromString("é-" + i));
                    row.setRowKind(RowKind.values()[(i + batchIndex) % 4]);
                    rows.add(row);
                    first.processElement(new StreamRecord<>(row, i + 100));
                }
                probeCount += first.extractOutputValues().size();
                for (var row : first.extractOutputValues()) lookup.processElement(new StreamRecord<>(row));
                first.getOutput().clear();
                for (var row : lookup.extractOutputValues()) {
                    expectedLookup.add(row);
                    last.processElement(new StreamRecord<>(row));
                }
                lookup.getOutput().clear();
                expectedOutput.addAll(last.extractOutputValues());
                last.getOutput().clear();
                var timestamps = new long[rows.size()];
                var present = new boolean[rows.size()];
                java.util.Arrays.fill(present, true);
                java.util.Arrays.setAll(timestamps, i -> i + 100);
                try (var input = ArrowRowDataBatch.transpose(rows, PROBE, allocator)
                        .withEnvelope(
                                rows.stream().map(RowData::getRowKind).toArray(RowKind[]::new), present, timestamps)) {
                    nativeHarness.accept(0, input);
                }
                assertThat(bytes(nativeHarness.outputRows.get(shared ? 1 : 0))).containsExactly(bytes(expectedOutput));
                if (shared) assertThat(bytes(nativeHarness.rows)).containsExactly(bytes(expectedLookup));
                for (var times : nativeHarness.outputTimes)
                    assertThat(times).isNotEmpty().containsOnlyNulls();
                assertThat(nativeHarness
                                .stageMetrics(LOOKUP)
                                .getIOMetricGroup()
                                .getNumRecordsInCounter()
                                .getCount())
                        .isEqualTo(probeCount);
                assertThat(nativeHarness
                                .stageMetrics(LOOKUP)
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .getCount())
                        .isEqualTo(expectedLookup.size());
            }
            nativeHarness.prepareSnapshotPreBarrier(7);
            nativeHarness.snapshot(7, 100);
            nativeHarness.end(0);
        }
    }

    private static final class Calc {
        final RowType type;
        final List<RexNode> projections;
        final RexNode condition;
        final byte[] plan;

        Calc(RowType type, List<Integer> columns, int id) {
            this.type = type;
            var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
            var rex = new RexBuilder(types);
            projections = columns.stream()
                    .map(i -> (RexNode) rex.makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(i)), i))
                    .collect(java.util.stream.Collectors.toList());
            condition = rex.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                    rex.makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(0)), 0));
            plan = StreamFusionNativeRegionTranslator.identifyStage(
                    StreamFusionCalcTranslator.createStagePlan(type, type, projections, condition), id);
        }

        OneInputStreamOperatorTestHarness<RowData, RowData> flink() throws Exception {
            var input = new Transformation<RowData>("input", InternalTypeInfo.of(type), 1) {
                protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                    return List.of(this);
                }

                public List<Transformation<?>> getInputs() {
                    return List.of();
                }
            };
            var factory = CalcCodeGenerator.generateCalcOperator(
                    new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                    input,
                    type,
                    scala.collection.JavaConverters.asScalaBufferConverter(projections)
                            .asScala()
                            .toSeq(),
                    scala.Option.apply(condition),
                    true,
                    "LookupCompositionParity");
            var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0);
            harness.setup(new RowDataSerializer(type));
            harness.open();
            return harness;
        }
    }
}
