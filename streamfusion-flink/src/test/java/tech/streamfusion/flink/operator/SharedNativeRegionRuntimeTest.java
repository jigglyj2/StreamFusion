/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
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
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.memory.FlinkManagedMemory;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.window.NativeLocalWindowResources;
import tech.streamfusion.proto.plan.v1.*;

/** Exercises the real Flink lifecycle and side-output dispatcher, with generated Flink compute. */
class SharedNativeRegionRuntimeTest {
    static final RowType TYPE = RowType.of(new IntType(), new VarCharType());
    static final RowType TEXT = RowType.of(new VarCharType());

    @Test
    void generatedChangelogMatchesFlinkOnBothExitsWithoutDuplicatingTheSharedStage() throws Exception {
        for (int seed : List.of(3, 19, 71)) {
            var expectedMain = new ArrayList<RowData>();
            var expectedSide = new ArrayList<RowData>();
            var rows = rows(seed);
            var memory = new FlinkManagedMemory[1];
            try (var allocator = new RootAllocator(64L << 20);
                    var harness = new NativeRegionTestHarness(factory(), List.of(TYPE, TEXT));
                    var first = flink(TYPE, TYPE, List.of(0, 1));
                    var branch = flink(TYPE, TEXT, List.of(1));
                    var last = flink(TEXT, TEXT, List.of(0))) {
                harness.open();
                memory[0] = memory(harness);
                for (int start = 0; start < rows.size(); start += 17) {
                    var chunk = rows.subList(start, Math.min(rows.size(), start + 17));
                    for (RowData row : chunk) first.processElement(new StreamRecord<>(row, start));
                    for (RowData row : first.extractOutputValues()) {
                        expectedMain.add(row);
                        branch.processElement(new StreamRecord<>(row));
                    }
                    first.getOutput().clear();
                    for (RowData row : branch.extractOutputValues()) last.processElement(new StreamRecord<>(row));
                    branch.getOutput().clear();
                    expectedSide.addAll(last.extractOutputValues());
                    last.getOutput().clear();
                    try (var batch = ArrowRowDataBatch.transpose(chunk, TYPE, allocator)
                            .withEnvelope(
                                    chunk.stream().map(RowData::getRowKind).toArray(RowKind[]::new),
                                    new boolean[chunk.size()],
                                    new long[chunk.size()])) {
                        if (seed == 19) {
                            var exchangeMemory = tech.streamfusion.flink.TestingNativeMemoryManager.create();
                            try (var envelope =
                                    tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(batch, TYPE)) {
                                for (var frame : tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge.route(
                                        NativeExchangePlanSerializer.singleton(TYPE),
                                        envelope.batch(),
                                        allocator,
                                        exchangeMemory)) {
                                    harness.metrics()
                                            .getIOMetricGroup()
                                            .getNumRecordsInCounter()
                                            .inc();
                                    harness.processElement(0, new StreamRecord<>(frame));
                                }
                            }
                            assertThat(exchangeMemory.available()).isEqualTo(exchangeMemory.limit());
                        } else harness.accept(0, batch);
                    }
                }
                assertThat(bytes(harness.rows, TYPE)).containsExactly(bytes(expectedMain, TYPE));
                assertThat(bytes(harness.outputRows.get(1), TEXT)).containsExactly(bytes(expectedSide, TEXT));
                assertThat(harness.outputTimes.get(0))
                        .hasSize(expectedMain.size())
                        .containsOnlyNulls();
                assertThat(harness.outputTimes.get(1))
                        .hasSize(expectedSide.size())
                        .containsOnlyNulls();
                for (long id : List.of(11L, 12L, 13L)) {
                    var io = harness.stageMetrics(id).getIOMetricGroup();
                    assertThat(io.getNumRecordsInCounter().getCount())
                            .isEqualTo(id == 11 ? rows.size() : expectedMain.size());
                    assertThat(io.getNumRecordsOutCounter().getCount())
                            .isEqualTo(id == 13 ? expectedSide.size() : expectedMain.size());
                }
                assertThat(harness.metrics()
                                .getIOMetricGroup()
                                .getNumRecordsInCounter()
                                .getCount())
                        .isEqualTo(rows.size());
                assertThat(harness.metrics()
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .getCount())
                        .isEqualTo(expectedMain.size() + expectedSide.size());
                harness.processWatermark(0, new Watermark(100));
                harness.processWatermarkStatus(0, WatermarkStatus.IDLE);
                harness.processWatermarkStatus(0, WatermarkStatus.ACTIVE);
                harness.processWatermark(0, new Watermark(200));
                // The Flink output broadcasts these once to all real outgoing edges.
                assertThat(harness.controls)
                        .containsExactly(
                                new Watermark(100), WatermarkStatus.IDLE, WatermarkStatus.ACTIVE, new Watermark(200));
                harness.prepareSnapshotPreBarrier(7);
                harness.snapshot(7, 200);
                harness.end(0);
                try (var empty = ArrowRowDataBatch.empty(TYPE, allocator)) {
                    assertThatThrownBy(() -> harness.accept(0, empty)).hasMessageContaining("ended");
                }
            }
            assertThat(memory[0].reserved()).isZero();
        }
    }

    @Test
    void downstreamFailureCancelsTheSharedInvocationAndReleasesManagedMemory() throws Exception {
        FlinkManagedMemory memory;
        try (var allocator = new RootAllocator(64L << 20);
                var harness = new NativeRegionTestHarness(factory(), List.of(TYPE, TEXT));
                var input = ArrowRowDataBatch.transpose(rows(3), TYPE, allocator)) {
            harness.open();
            memory = memory(harness);
            harness.sinkFailure = new IllegalStateException("shared sink failed");
            assertThatThrownBy(() -> harness.accept(0, input)).hasMessageContaining("shared sink failed");
            assertThatThrownBy(() -> harness.accept(0, input)).hasMessageContaining("active or failed invocation");
        }
        assertThat(memory.reserved()).isZero();
    }

    static NativeRegionPlan plan() throws Exception {
        var plan = NativeRegionPlan.newBuilder()
                .setProtocolVersion(1)
                .setInputCount(1)
                .addOutputStageIds(11)
                .addOutputStageIds(13);
        for (int index = 0; index < 3; index++) {
            var in = index == 2 ? TEXT : TYPE;
            var out = index == 0 ? TYPE : TEXT;
            var columns = index == 0 ? List.of(0, 1) : index == 1 ? List.of(1) : List.of(0);
            var stage = NativePlan.parseFrom(StreamFusionCalcTranslator.createStagePlan(
                            in, out, projections(in, columns), condition(in, out)))
                    .getRoot();
            plan.addStages(NativeRegionStage.newBuilder()
                    .setOperator(stage.toBuilder().setPlanNodeId(11 + index))
                    .addInputs(
                            index == 0
                                    ? NativeRegionInputReference.newBuilder().setExternalInput(0)
                                    : NativeRegionInputReference.newBuilder().setStageId(10 + index)));
        }
        return plan.build();
    }

    static StreamFusionNativeRegionOperatorFactory factory() throws Exception {
        return StreamFusionNativeRegionOperatorFactory.shared(
                List.of(TYPE),
                List.of(TYPE, TEXT),
                plan().toByteArray(),
                List.of(),
                List.of(NativeExchangePlanSerializer.singleton(TYPE)),
                NativeLocalWindowResources.NONE);
    }

    private static FlinkManagedMemory memory(NativeRegionTestHarness harness) throws Exception {
        var field = StreamFusionArrowNativeRegionOperator.class.getDeclaredField("memory");
        field.setAccessible(true);
        return (FlinkManagedMemory) ((StreamFusionTaskMemory) field.get(harness.region())).nativeMemoryManager();
    }

    private static RexNode condition(RowType input, RowType output) {
        if (!input.equals(output)) return null;
        var types =
                new FlinkTypeFactory(SharedNativeRegionRuntimeTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);
        return new RexBuilder(types)
                .makeCall(
                        org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                        projections(input, List.of(0)).get(0));
    }

    private static List<RexNode> projections(RowType type, List<Integer> columns) {
        var types =
                new FlinkTypeFactory(SharedNativeRegionRuntimeTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        return columns.stream()
                .map(column -> (RexNode)
                        rex.makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(column)), column))
                .collect(java.util.stream.Collectors.toList());
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> flink(
            RowType inputType, RowType outputType, List<Integer> columns) throws Exception {
        var input = new Transformation<RowData>("input", InternalTypeInfo.of(inputType), 1) {
            protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            public List<Transformation<?>> getInputs() {
                return List.of();
            }
        };
        var factory = CalcCodeGenerator.generateCalcOperator(
                new CodeGeneratorContext(new Configuration(), SharedNativeRegionRuntimeTest.class.getClassLoader()),
                input,
                outputType,
                scala.collection.JavaConverters.asScalaBufferConverter(projections(inputType, columns))
                        .asScala()
                        .toSeq(),
                scala.Option.apply(condition(inputType, outputType)),
                true,
                "SharedExitParity");
        var result = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0);
        result.setup(new RowDataSerializer(outputType));
        result.open();
        return result;
    }

    private static List<RowData> rows(int seed) {
        var random = new Random(seed);
        var rows = new ArrayList<RowData>();
        for (int i = 0; i < 113; i++) {
            var row = GenericRowData.of(
                    i % 5 == 0 ? null : random.nextInt(),
                    i % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(99)));
            row.setRowKind(RowKind.values()[i % 4]);
            rows.add(row);
        }
        return rows;
    }

    private static byte[] bytes(List<RowData> rows, RowType type) throws Exception {
        var bytes = new DataOutputSerializer(128);
        var serializer = new RowDataSerializer(type);
        for (var row : rows) serializer.serialize(row, bytes);
        return bytes.getCopyOfBuffer();
    }
}
