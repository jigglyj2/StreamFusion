/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExpandCodeGenerator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.TestingNativeMemoryManager;
import tech.streamfusion.flink.arrow.ArrowCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.NativeCalcResult;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.expand.StreamFusionExpandTranslator;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Uses the actual Flink code generators, not a Java reimplementation of the SQL operations. */
class GeneratedNativeRegionParityTest {
    private static final RowType TYPE = RowType.of(new IntType(), new VarCharType());
    private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    private final RexBuilder rex = new RexBuilder(types);
    private final RexNode id = rex.makeInputRef(types.createFieldTypeFromLogicalType(TYPE.getTypeAt(0)), 0);
    private final RexNode text = rex.makeInputRef(types.createFieldTypeFromLogicalType(TYPE.getTypeAt(1)), 1);
    private final List<RexNode> identity = List.of(id, text);
    private final List<List<RexNode>> alternatives = List.of(
            identity, List.of(id, rex.makeNullLiteral(types.createFieldTypeFromLogicalType(TYPE.getTypeAt(1)))));
    private final RexNode lower =
            rex.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, id, rex.makeExactLiteral(BigDecimal.ZERO));
    private final RexNode upper =
            rex.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, id, rex.makeExactLiteral(BigDecimal.valueOf(5)));

    @Test
    void branchingTreeMatchesFlinkCodegenChangelogsAndEveryPhysicalStageCount() throws Exception {
        var union = tech.streamfusion.proto.plan.v1.Operator.newBuilder()
                .setUnion(tech.streamfusion.proto.plan.v1.Union.newBuilder()
                        .addInputs(edge(0))
                        .addInputs(edge(1)))
                .build();
        byte[] leftPlan = StreamFusionNativeRegionTranslator.composeWithInputs(
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, lower), 27),
                List.of(serialized(edge(0))));
        byte[] rightPlan = StreamFusionNativeRegionTranslator.composeWithInputs(
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, upper), 43),
                List.of(serialized(edge(1))));
        byte[] branchPlan = StreamFusionNativeRegionTranslator.composeWithInputs(
                StreamFusionNativeRegionTranslator.identifyStage(serialized(union), 59), List.of(leftPlan, rightPlan));
        byte[] plan = StreamFusionNativeRegionTranslator.composeAbove(
                branchPlan,
                List.of(StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, upper), 71)));
        for (int seed = 0; seed < 3; seed++) {
            List<RowData> leftRows = seed == 2 ? List.of() : rows(seed);
            List<RowData> rightRows = rows(seed + 7);
            List<RowData> leftFiltered = flinkCalc(leftRows, lower);
            List<RowData> rightFiltered = flinkCalc(rightRows, upper);
            List<RowData> combined = new ArrayList<>(leftFiltered);
            combined.addAll(rightFiltered);
            List<RowData> expected = flinkCalc(combined, upper);
            var memory = TestingNativeMemoryManager.create();
            try (var allocator = new RootAllocator(64L << 20);
                    var context = new NativeExecutionContext(plan, memory);
                    var left = ArrowRowDataBatch.transpose(leftRows, TYPE, allocator)
                            .withEnvelope(
                                    leftRows.stream().map(RowData::getRowKind).toArray(RowKind[]::new),
                                    new boolean[leftRows.size()],
                                    new long[leftRows.size()]);
                    var right = ArrowRowDataBatch.transpose(rightRows, TYPE, allocator)
                            .withEnvelope(
                                    rightRows.stream().map(RowData::getRowKind).toArray(RowKind[]::new),
                                    new boolean[rightRows.size()],
                                    new long[rightRows.size()])) {
                var inputs = List.of(left, right);
                var bridge = new tech.streamfusion.flink.arrow.ArrowNativePlanBridge(context, TYPE, allocator);
                List<RowData> actual = new ArrayList<>();
                try (var stream = bridge.executeStream(inputs)) {
                    NativeCalcResult result;
                    while ((result = stream.nextWithSelection()) != null) {
                        try (var owned = result) {
                            var output = owned.selectEnvelopeFrom(inputs);
                            for (int row = 0; row < output.size(); row++) {
                                RowData copy = new RowDataSerializer(TYPE).copy(output.rowView(row));
                                copy.setRowKind(output.rowKind(row));
                                actual.add(copy);
                            }
                        }
                    }
                }
                assertThat(bytes(actual)).containsExactly(bytes(expected));
                assertThat(context.metricSnapshot())
                        .containsExactly(
                                (1L << 32) | 71,
                                combined.size(),
                                expected.size(),
                                (1L << 32) | 59,
                                combined.size(),
                                combined.size(),
                                (1L << 32) | 27,
                                leftRows.size(),
                                leftFiltered.size(),
                                1,
                                0,
                                leftRows.size(),
                                (1L << 32) | 43,
                                rightRows.size(),
                                rightFiltered.size(),
                                2,
                                0,
                                rightRows.size());
                // Flink delivers ports independently, not as a synchronized pair. Reuse the
                // same native tree and verify reversed arrival order with empty other ports.
                combined.clear();
                combined.addAll(rightFiltered);
                combined.addAll(leftFiltered);
                actual.clear();
                try (var dispatcher = new tech.streamfusion.flink.arrow.ArrowNativePlanDispatcher(
                        context, List.of(TYPE, TYPE), TYPE, allocator)) {
                    java.util.function.Consumer<ArrowRowDataBatch> capture = output -> {
                        for (int row = 0; row < output.size(); row++) {
                            RowData copy = new RowDataSerializer(TYPE).copy(output.rowView(row));
                            copy.setRowKind(output.rowKind(row));
                            actual.add(copy);
                        }
                    };
                    dispatcher.process(1, right, capture);
                    dispatcher.process(0, left, capture);
                }
                assertThat(bytes(actual)).containsExactly(bytes(flinkCalc(combined, upper)));
                try (var harness = new NativeRegionTestHarness(plan, List.of(TYPE, TYPE), TYPE)) {
                    harness.open();
                    try (var envelope = tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(right, TYPE)) {
                        byte[] exchange = tech.streamfusion.flink.exchange.NativeExchangePlanSerializer.singleton(TYPE);
                        for (var frame : tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge.route(
                                exchange, envelope.batch(), allocator, memory)) {
                            harness.metrics()
                                    .getIOMetricGroup()
                                    .getNumRecordsInCounter()
                                    .inc();
                            harness.processElement(1, new StreamRecord<>(frame));
                        }
                    }
                    harness.accept(0, left);
                    assertThat(bytes(harness.rows)).containsExactly(bytes(flinkCalc(combined, upper)));
                    assertThat(harness.metrics()
                                    .getIOMetricGroup()
                                    .getNumRecordsInCounter()
                                    .getCount())
                            .isEqualTo(leftRows.size() + rightRows.size());
                    assertThat(harness.metrics()
                                    .getIOMetricGroup()
                                    .getNumRecordsOutCounter()
                                    .getCount())
                            .isEqualTo(harness.rows.size());
                    var rootMetrics = harness.stageMetrics((1L << 32) | 71).getIOMetricGroup();
                    assertThat(rootMetrics.getNumRecordsInCounter().getCount()).isEqualTo(combined.size());
                    assertThat(rootMetrics.getNumRecordsOutCounter().getCount()).isEqualTo(harness.rows.size());
                    assertThat(harness.stageMetrics((1L << 32) | 27)
                                    .getIOMetricGroup()
                                    .getNumRecordsInCounter()
                                    .getCount())
                            .isEqualTo(leftRows.size());
                    assertThat(harness.stageMetrics((1L << 32) | 43)
                                    .getIOMetricGroup()
                                    .getNumRecordsInCounter()
                                    .getCount())
                            .isEqualTo(rightRows.size());
                    harness.processWatermark(1, new org.apache.flink.streaming.api.watermark.Watermark(60));
                    assertThat(harness.controls).isEmpty();
                    harness.processWatermark(0, new org.apache.flink.streaming.api.watermark.Watermark(50));
                    harness.processWatermarkStatus(
                            0, org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
                    assertThat(harness.controls)
                            .containsExactly(
                                    new org.apache.flink.streaming.api.watermark.Watermark(50),
                                    new org.apache.flink.streaming.api.watermark.Watermark(60));
                    harness.snapshot(1, 0);
                    harness.end(0);
                    assertThatThrownBy(() -> harness.accept(0, left)).hasMessageContaining("ended");
                }
            }
            assertThat(memory.available()).isEqualTo(memory.limit());
        }
    }

    private static tech.streamfusion.proto.plan.v1.Operator edge(int index) {
        return tech.streamfusion.proto.plan.v1.Operator.newBuilder()
                .setInput(tech.streamfusion.proto.plan.v1.Input.newBuilder().setInputIndex(index))
                .build();
    }

    private static byte[] serialized(tech.streamfusion.proto.plan.v1.Operator root) {
        return tech.streamfusion.proto.plan.v1.NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(root)
                .build()
                .toByteArray();
    }

    @Test
    void mixedRegionMatchesGeneratedFlinkChangelogsAndEveryStageRecordCount() throws Exception {
        for (int seed = 0; seed < 4; seed++) {
            boolean physicalIds = seed % 2 == 1;
            byte[] plan;
            if (physicalIds) {
                List<byte[]> fragments = stagePlans();
                plan = StreamFusionNativeRegionTranslator.compose(List.of(
                        StreamFusionNativeRegionTranslator.identifyStage(fragments.get(0), 27),
                        StreamFusionNativeRegionTranslator.identifyStage(fragments.get(1), 43),
                        StreamFusionNativeRegionTranslator.identifyStage(fragments.get(2), 59)));
            } else {
                plan = plan();
            }
            List<RowData> inputs = rows(seed);
            List<RowData> first = flinkCalc(inputs, lower);
            List<RowData> expanded = flinkExpand(first);
            List<RowData> expected = flinkCalc(expanded, upper);
            DataOutputSerializer actual = new DataOutputSerializer(1024);
            var manager = TestingNativeMemoryManager.create();
            long initialAvailable = manager.available();
            try (RootAllocator allocator = new RootAllocator(64L << 20);
                    NativeExecutionContext context = new NativeExecutionContext(plan, manager)) {
                var execution = new tech.streamfusion.flink.arrow.ArrowNativePlanBridge(context, TYPE, allocator);
                int size = new int[] {1, 7, 63, 257}[seed];
                for (int start = 0; start < inputs.size(); start += size) {
                    List<RowData> rows = inputs.subList(start, Math.min(start + size, inputs.size()));
                    try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, TYPE, allocator)
                                    .withEnvelope(
                                            rows.stream()
                                                    .map(RowData::getRowKind)
                                                    .toArray(RowKind[]::new),
                                            new boolean[rows.size()],
                                            new long[rows.size()]);
                            var stream = execution.executeStream(List.of(input))) {
                        NativeCalcResult next;
                        while ((next = stream.nextWithSelection()) != null) {
                            try (var result = next) {
                                ArrowRowDataBatch output = result.selectEnvelopeFrom(input);
                                RowDataSerializer serializer = new RowDataSerializer(TYPE);
                                for (int row = 0; row < output.size(); row++) {
                                    RowData view = output.rowView(row);
                                    view.setRowKind(output.rowKind(row));
                                    serializer.serialize(view, actual);
                                }
                            }
                        }
                    }
                }
                // Physical IDs come from Java; anonymous legacy plans retain preorder IDs.
                assertThat(context.metricSnapshot())
                        .containsExactly(
                                physicalIds ? (1L << 32) | 59 : 1,
                                expanded.size(),
                                expected.size(),
                                physicalIds ? (1L << 32) | 43 : 2,
                                first.size(),
                                expanded.size(),
                                physicalIds ? (1L << 32) | 27 : 3,
                                inputs.size(),
                                first.size(),
                                physicalIds ? 1 : 4,
                                0,
                                inputs.size());
            }
            byte[] actualBytes = actual.getCopyOfBuffer();
            byte[] expectedBytes = bytes(expected);
            int mismatch = java.util.Arrays.mismatch(actualBytes, expectedBytes);
            assertThat(mismatch)
                    .as(
                            "generated seed %s: byte mismatch at %s (actual %s, expected %s)",
                            seed,
                            mismatch,
                            mismatch >= 0 && mismatch < actualBytes.length ? actualBytes[mismatch] : "end",
                            mismatch >= 0 && mismatch < expectedBytes.length ? expectedBytes[mismatch] : "end")
                    .isEqualTo(-1);
            assertThat(manager.available()).isEqualTo(initialAvailable);
        }
    }

    @Test
    void chunkedExpandPreservesFlinkChangelogAndStageCountsThroughTheSharedStream() throws Exception {
        List<RowData> inputs = new ArrayList<>();
        for (int index = 0; index < 6003; index++) {
            GenericRowData row = GenericRowData.of(index % 6, StringData.fromString("尾-" + index));
            row.setRowKind(RowKind.values()[index % 4]);
            inputs.add(row);
        }
        List<RowData> first = flinkCalc(inputs, lower);
        List<RowData> expanded = flinkExpand(first);
        List<RowData> expected = flinkCalc(expanded, upper);
        DataOutputSerializer actual = new DataOutputSerializer(1024);
        var manager = TestingNativeMemoryManager.create();
        long initialAvailable = manager.available();
        int batches = 0;
        try (RootAllocator allocator = new RootAllocator(64L << 20);
                NativeExecutionContext context = new NativeExecutionContext(plan(), manager);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(inputs, TYPE, allocator)
                        .withEnvelope(
                                inputs.stream().map(RowData::getRowKind).toArray(RowKind[]::new),
                                new boolean[inputs.size()],
                                new long[inputs.size()]);
                ArrowCDataBridge.NativeOutputStream stream =
                        new ArrowCDataBridge.ReusableExecution(context, TYPE, allocator).executeStream(input)) {
            NativeCalcResult next;
            while ((next = stream.nextWithSelection()) != null) {
                try (NativeCalcResult result = next) {
                    ArrowRowDataBatch output = result.selectEnvelopeFrom(input);
                    assertThat(output.size()).isLessThanOrEqualTo(4096);
                    RowDataSerializer serializer = new RowDataSerializer(TYPE);
                    for (int row = 0; row < output.size(); row++) {
                        RowData view = output.rowView(row);
                        view.setRowKind(output.rowKind(row));
                        serializer.serialize(view, actual);
                    }
                    batches++;
                }
            }
            assertThat(context.metricSnapshot())
                    .containsExactly(
                            1,
                            expanded.size(),
                            expected.size(),
                            2,
                            first.size(),
                            expanded.size(),
                            3,
                            inputs.size(),
                            first.size(),
                            4,
                            0,
                            inputs.size());
        }
        assertThat(batches).isGreaterThan(1);
        assertThat(actual.getCopyOfBuffer()).containsExactly(bytes(expected));
        assertThat(manager.available()).isEqualTo(initialAvailable);
    }

    @Test
    void regionRejectsNestedOrUnsupportedStageContracts() {
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.compose(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.compose(List.of(plan())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void computedExpandProjectionsMatchFlinkThroughTheSharedTree() throws Exception {
        RexNode two = rex.makeExactLiteral(BigDecimal.valueOf(2));
        var repeat = org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.REPEAT;
        List<List<RexNode>> projects = List.of(
                List.of(
                        rex.makeCall(SqlStdOperatorTable.PLUS, id, rex.makeExactLiteral(BigDecimal.ONE)),
                        rex.makeCall(repeat, text, two)),
                List.of(id, rex.makeCall(repeat, rex.makeLiteral("é"), two)),
                alternatives.get(1));
        byte[] plan = StreamFusionNativeRegionTranslator.compose(List.of(
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, lower), 27),
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionExpandTranslator.createStagePlan(TYPE, TYPE, new ArrayList<>(projects)), 43),
                StreamFusionNativeRegionTranslator.identifyStage(
                        StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, upper), 59)));
        for (int seed = 0; seed < 4; seed++) {
            List<RowData> rows = rows(seed);
            List<RowData> first = flinkCalc(rows, lower);
            List<RowData> expanded = flinkExpand(first, projects);
            List<RowData> expected = flinkCalc(expanded, upper);
            var memory = TestingNativeMemoryManager.create();
            long available = memory.available();
            var actual = new DataOutputSerializer(1024);
            try (var allocator = new RootAllocator(64L << 20);
                    var context = new NativeExecutionContext(plan, memory)) {
                var execution = new ArrowCDataBridge.ReusableExecution(context, TYPE, allocator);
                int size = new int[] {1, 7, 127, 257}[seed];
                for (int start = 0; start < rows.size(); start += size) {
                    List<RowData> chunk = rows.subList(start, Math.min(start + size, rows.size()));
                    try (var input = ArrowRowDataBatch.transpose(chunk, TYPE, allocator)
                                    .withEnvelope(
                                            chunk.stream()
                                                    .map(RowData::getRowKind)
                                                    .toArray(RowKind[]::new),
                                            new boolean[chunk.size()],
                                            new long[chunk.size()]);
                            var stream = execution.executeStream(input)) {
                        NativeCalcResult result;
                        while ((result = stream.nextWithSelection()) != null) {
                            try (var owned = result) {
                                var output = owned.selectEnvelopeFrom(input);
                                var serializer = new RowDataSerializer(TYPE);
                                for (int row = 0; row < output.size(); row++) {
                                    RowData view = output.rowView(row);
                                    view.setRowKind(output.rowKind(row));
                                    serializer.serialize(view, actual);
                                }
                            }
                        }
                    }
                }
                assertThat(context.metricSnapshot())
                        .containsExactly(
                                (1L << 32) | 59,
                                expanded.size(),
                                expected.size(),
                                (1L << 32) | 43,
                                first.size(),
                                expanded.size(),
                                (1L << 32) | 27,
                                rows.size(),
                                first.size(),
                                1,
                                0,
                                rows.size());
            }
            assertThat(actual.getCopyOfBuffer()).as("seed %s", seed).containsExactly(bytes(expected));
            assertThat(memory.available()).isEqualTo(available);
        }
    }

    @Test
    void mixedRegionCreatesOnlyOneArrowRuntimeOperator() {
        Transformation<RowData> translated =
                StreamFusionNativeRegionTranslator.translate(inputTransformation(), TYPE, TYPE, stagePlans());
        var nativeStages = translated.getTransitivePredecessors().stream()
                .filter(stage -> stage instanceof org.apache.flink.streaming.api.transformations.OneInputTransformation)
                .map(stage -> (org.apache.flink.streaming.api.transformations.OneInputTransformation<?, ?>) stage)
                .filter(stage -> stage.getOperator() instanceof StreamFusionArrowNativeOperator)
                .collect(java.util.stream.Collectors.toList());
        assertThat(nativeStages).hasSize(1);
        assertThat(nativeStages.get(0).getOutputType())
                .isSameAs(tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo.INSTANCE);
        assertThat(nativeStages.get(0).getInputType())
                .isSameAs(tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo.INSTANCE);
    }

    private byte[] plan() {
        return StreamFusionNativeRegionTranslator.compose(stagePlans());
    }

    private List<byte[]> stagePlans() {
        return List.of(
                StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, lower),
                StreamFusionExpandTranslator.createStagePlan(
                        TYPE,
                        TYPE,
                        List.of(new ArrayList<>(alternatives.get(0)), new ArrayList<>(alternatives.get(1)))),
                StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, identity, upper));
    }

    private List<RowData> flinkCalc(List<RowData> rows, RexNode condition) throws Exception {
        Transformation<RowData> input = inputTransformation();
        var factory = CalcCodeGenerator.generateCalcOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                input,
                TYPE,
                scala.collection.JavaConverters.asScalaBufferConverter(identity)
                        .asScala()
                        .toSeq(),
                scala.Option.apply(condition),
                true,
                "RegionCalcParity");
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0)) {
            harness.setup(new RowDataSerializer(TYPE));
            harness.open();
            for (RowData row : rows) {
                harness.processElement(new StreamRecord<>(row));
            }
            return new ArrayList<>(harness.extractOutputValues());
        }
    }

    private List<RowData> flinkExpand(List<RowData> rows) throws Exception {
        return flinkExpand(rows, alternatives);
    }

    private List<RowData> flinkExpand(List<RowData> rows, List<List<RexNode>> projects) throws Exception {
        var factory = ExpandCodeGenerator.generateExpandOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                TYPE,
                TYPE,
                projects,
                true,
                "RegionExpandParity");
        try (var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 1, 1, 0)) {
            harness.setup(new RowDataSerializer(TYPE));
            harness.open();
            for (RowData row : rows) {
                harness.processElement(new StreamRecord<>(row));
            }
            return new ArrayList<>(harness.extractOutputValues());
        }
    }

    private static List<RowData> rows(int seed) {
        Random random = new Random(1781 + seed);
        List<RowData> result = new ArrayList<>();
        for (int index = 0; index < 257; index++) {
            GenericRowData row = GenericRowData.of(
                    index % 11 == 0 ? null : random.nextInt(19) - 7,
                    index % 7 == 0 ? null : StringData.fromString("é-" + random.nextInt(99)));
            row.setRowKind(RowKind.values()[index % 4]);
            result.add(row);
        }
        return result;
    }

    private static byte[] bytes(List<RowData> rows) throws Exception {
        DataOutputSerializer bytes = new DataOutputSerializer(1024);
        RowDataSerializer serializer = new RowDataSerializer(TYPE);
        for (RowData row : rows) {
            serializer.serialize(row, bytes);
        }
        return bytes.getCopyOfBuffer();
    }

    private static Transformation<RowData> inputTransformation() {
        return new Transformation<RowData>("input", InternalTypeInfo.of(TYPE), 1) {
            @Override
            protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            @Override
            public List<Transformation<?>> getInputs() {
                return List.of();
            }
        };
    }
}
