/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;

/** Checks mixed-width comparison coercion against Flink, including arithmetic before widening. */
class MixedIntegerComparisonParityTest {
    private static final RowType INPUT =
            RowType.of(new TinyIntType(), new SmallIntType(), new IntType(), new BigIntType());

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void integerComparisonPreservesOverflowNullsChangelogAndMetrics(boolean rocks) throws Exception {
        var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        var inputs = new ArrayList<RexNode>();
        for (int i = 0; i < INPUT.getFieldCount(); i++) {
            inputs.add(rex.makeInputRef(types.createFieldTypeFromLogicalType(INPUT.getTypeAt(i)), i));
        }
        var projections = new ArrayList<RexNode>();
        var overflow = rex.makeCall(SqlStdOperatorTable.PLUS, inputs.get(2), rex.makeExactLiteral(BigDecimal.ONE));
        projections.add(overflow);
        for (var left : List.of(inputs.get(0), inputs.get(1), inputs.get(2), overflow)) {
            var right = inputs.get(3);
            for (var operator : List.of(
                    SqlStdOperatorTable.EQUALS,
                    SqlStdOperatorTable.NOT_EQUALS,
                    SqlStdOperatorTable.LESS_THAN,
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    SqlStdOperatorTable.GREATER_THAN,
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    SqlStdOperatorTable.IS_DISTINCT_FROM,
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM)) {
                projections.add(rex.makeCall(operator, left, right));
                projections.add(rex.makeCall(operator, right, left));
            }
        }
        var output = RowType.of(projections.stream()
                .map(expression -> FlinkTypeFactory.toLogicalType(expression.getType()))
                .toArray(LogicalType[]::new));
        byte[] plan = StreamFusionNativeRegionTranslator.identifyStage(
                StreamFusionCalcTranslator.createStagePlan(INPUT, output, projections, null),
                1,
                "integer-comparison",
                "integer-comparison");
        try (var oracle = oracle(output, projections);
                var nativePlan = new KeyedNativeMetricHarness(rocks, plan, List.of(INPUT), output, List.of());
                var allocator = new RootAllocator(64L << 20)) {
            var expected = new DataOutputSerializer(128);
            var random = new Random(197);
            long[] edges = {Long.MIN_VALUE, Long.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1};
            for (int count : new int[] {0, 1, 31, 3001}) {
                var rows = new ArrayList<GenericRowData>();
                var present = new boolean[count];
                var times = new long[count];
                for (int index = 0; index < count; index++) {
                    long value = index < edges.length ? edges[index] : random.nextLong();
                    var row = GenericRowData.of(
                            index % 7 == 6 ? null : (byte) value,
                            index % 11 == 10 ? null : (short) value,
                            index % 13 == 12 ? null : (int) value,
                            index % 17 == 16 ? null : index % 3 == 0 ? (long) (int) value : value);
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
                "IntegerComparisonOracle");
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 16, 1, 0);
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return new FlinkStageMetricOracle(harness);
    }
}
