/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.flink.streaming.api.watermark.Watermark;
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
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;

/** Nested-field conditional evaluation against Flink, with large unreferenced sibling payloads. */
class NestedConditionalRuntimeParityTest {
    private static final RowType INNER =
            RowType.of(new LogicalType[] {new BigIntType(), new VarCharType()}, new String[] {"v", "unused"});
    private static final RowType PAYLOAD =
            RowType.of(new LogicalType[] {INNER, new VarCharType()}, new String[] {"nested", "unused"});
    private static final RowType INPUT = RowType.of(
            new LogicalType[] {PAYLOAD, new BooleanType(), new VarCharType()},
            new String[] {"payload", "selected", "outside"});

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nestedConditionalsPreserveChangelogControlsAndMetrics(boolean rocks) throws Exception {
        var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        var payload = rex.makeInputRef(types.createFieldTypeFromLogicalType(PAYLOAD), 0);
        var value = rex.makeFieldAccess(rex.makeFieldAccess(payload, 0), 0);
        var selected = rex.makeInputRef(types.createFieldTypeFromLogicalType(new BooleanType()), 1);
        var low = rex.makeCall(
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                value,
                rex.makeExactLiteral(java.math.BigDecimal.valueOf(10000), value.getType()));
        var high = rex.makeCall(
                SqlStdOperatorTable.LESS_THAN,
                value,
                rex.makeExactLiteral(java.math.BigDecimal.valueOf(1000000), value.getType()));
        var range = rex.makeCall(SqlStdOperatorTable.AND, low, high);
        var projections = new ArrayList<RexNode>();
        projections.add(range);
        projections.add(rex.makeCall(SqlStdOperatorTable.OR, low, high));
        projections.add(rex.makeCall(SqlStdOperatorTable.AND, selected, range));
        projections.add(rex.makeCall(SqlStdOperatorTable.OR, selected, range));
        projections.add(rex.makeCall(SqlStdOperatorTable.CASE, selected, range, rex.makeLiteral(false)));
        projections.add(value);
        var output = RowType.of(projections.stream()
                .map(expression -> FlinkTypeFactory.toLogicalType(expression.getType()))
                .toArray(LogicalType[]::new));
        byte[] plan = StreamFusionNativeRegionTranslator.identifyStage(
                StreamFusionCalcTranslator.createStagePlan(INPUT, output, projections, null),
                1,
                "nested-conditional",
                "nested-conditional");
        try (var oracle = oracle(output, projections);
                var nativePlan = new KeyedNativeMetricHarness(rocks, plan, List.of(INPUT), output, List.of());
                var allocator = new RootAllocator(64L << 20)) {
            var expected = new DataOutputSerializer(128);
            var random = new Random(197);
            long[] edges = {Long.MIN_VALUE, Long.MAX_VALUE, 9999, 10000, 999999, 1000000, 0};
            var unused = StringData.fromString("é-unused".repeat(256));
            for (int count : new int[] {0, 1, 31, 3001}) {
                var rows = new ArrayList<GenericRowData>();
                var present = new boolean[count];
                var times = new long[count];
                for (int index = 0; index < count; index++) {
                    Long number =
                            index % 11 == 0 ? null : index < edges.length ? edges[index] : random.nextLong() % 2000000;
                    var inner = index % 7 == 0 ? null : GenericRowData.of(number, unused);
                    var parent = index % 13 == 0 ? null : GenericRowData.of(inner, unused);
                    var row = GenericRowData.of(parent, index % 17 == 0 ? null : index % 3 != 0, unused);
                    row.setRowKind(RowKind.values()[index % 4]);
                    rows.add(row);
                    present[index] = index % 3 != 0;
                    times[index] = index - 10;
                    oracle.accept(present[index] ? new StreamRecord<>(row, times[index]) : new StreamRecord<>(row));
                }
                for (var event : oracle.drain()) StageEventBytes.encode(output, event, expected);
                try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator)
                        .withEnvelope(
                                rows.stream().map(GenericRowData::getRowKind).toArray(RowKind[]::new),
                                present,
                                times)) {
                    nativePlan.processElement(0, new StreamRecord<>(batch));
                }
                var watermark = new Watermark(count);
                oracle.accept(watermark);
                for (var event : oracle.drain()) StageEventBytes.encode(output, event, expected);
                nativePlan.processWatermark(0, watermark);
                nativePlan.drainControls();
                assertThat(nativePlan.output.getCopyOfBuffer()).containsExactly(expected.getCopyOfBuffer());
                RegisteredMetricSurface.compare(
                        RegisteredMetricSurface.metrics(oracle.group()),
                        RegisteredMetricSurface.metrics(nativePlan.stage((1L << 32) | 1)));
            }
        }
    }

    private FlinkStageMetricOracle oracle(RowType output, List<RexNode> projections) throws Exception {
        var input = new Transformation<RowData>("input", InternalTypeInfo.of(INPUT), 1) {
            @Override
            protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            @Override
            public List<Transformation<?>> getInputs() {
                return List.of();
            }
        };
        var factory = CalcCodeGenerator.generateCalcOperator(
                new CodeGeneratorContext(new Configuration(), getClass().getClassLoader()),
                input,
                output,
                scala.collection.JavaConverters.asScalaBufferConverter(projections)
                        .asScala()
                        .toSeq(),
                scala.Option.empty(),
                true,
                "NestedConditionalOracle");
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 16, 1, 0);
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return new FlinkStageMetricOracle(harness);
    }
}
