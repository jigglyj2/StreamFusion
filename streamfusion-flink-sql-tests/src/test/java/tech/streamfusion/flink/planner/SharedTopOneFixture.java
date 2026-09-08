/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.flink.topn.StreamFusionTopNTranslator;

/** Same SQL-selected FastTop1 comparator in a Calc -> Top-1 -> Calc native tree. */
final class SharedTopOneFixture {
    final TopOneFlinkFixture top;
    private final Configuration config = new Configuration();
    private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    private final RexBuilder rex = new RexBuilder(types);

    SharedTopOneFixture(boolean ascending, boolean rank, boolean before) throws Exception {
        top = new TopOneFlinkFixture(ascending, rank, before);
    }

    static long id(int stage) {
        return (1L << 32) | (stage + 1);
    }

    static String name(int stage) {
        return "top-one-conformance-" + stage;
    }

    static String uid(int stage) {
        return "top-one-conformance-uid-" + stage;
    }

    static OperatorID operatorId(int stage) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(uid(stage)));
    }

    private List<RexNode> projections(RowType type) {
        var projections = new ArrayList<RexNode>();
        for (int field = 0; field < type.getFieldCount(); field++)
            projections.add(rex.makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(field)), field));
        return projections;
    }

    private RexNode condition(int stage, RowType type) {
        var column = projections(type).get(1);
        return rex.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
                rex.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL, column),
                rex.makeCall(
                        stage == 0
                                ? org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
                                : org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN,
                        column,
                        rex.makeExactLiteral(java.math.BigDecimal.valueOf(stage == 0 ? -1 : 9))));
    }

    byte[] plan() {
        var fragments = new ArrayList<byte[]>();
        for (int stage = 0; stage < 3; stage++) {
            RowType type = stage == 0 ? top.input : top.output;
            byte[] fragment = stage == 1
                    ? StreamFusionTopNTranslator.createStagePlan(
                            top.input,
                            top.output,
                            new int[] {0},
                            SortSpec.builder()
                                    .addField(1, top.ascending, true)
                                    .addField(2, true, false)
                                    .build(),
                            new int[0],
                            1,
                            1L,
                            null,
                            top.rankNumber,
                            top.before,
                            "APPEND_FAST",
                            0,
                            config)
                    : StreamFusionCalcTranslator.createStagePlan(type, type, projections(type), condition(stage, type));
            fragments.add(
                    StreamFusionNativeRegionTranslator.identifyStage(fragment, stage + 1, name(stage), uid(stage)));
        }
        return StreamFusionNativeRegionTranslator.compose(fragments);
    }

    FlinkStageMetricOracle oracle(int stage, boolean rocks) throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness;
        if (stage == 1) {
            harness = top.harness(rocks, 1, 0);
        } else {
            RowType type = stage == 0 ? top.input : top.output;
            var factory = CalcCodeGenerator.generateCalcOperator(
                    new CodeGeneratorContext(config, getClass().getClassLoader()),
                    input(type),
                    type,
                    scala.collection.JavaConverters.asScalaBufferConverter(projections(type))
                            .asScala()
                            .toSeq(),
                    scala.Option.apply(condition(stage, type)),
                    true,
                    "TopOneConformanceCalc" + stage);
            harness = new OneInputStreamOperatorTestHarness<>(factory, 16, 1, 0);
        }
        harness.getStreamConfig().setOperatorID(operatorId(stage));
        harness.getStreamConfig().setOperatorName(name(stage));
        harness.setup(new RowDataSerializer(stage == 0 ? top.input : top.output));
        harness.open();
        return new FlinkStageMetricOracle(harness);
    }

    private static org.apache.flink.api.dag.Transformation<RowData> input(RowType type) {
        return new org.apache.flink.api.dag.Transformation<RowData>(
                "input", org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(type), 1) {
            @Override
            protected List<org.apache.flink.api.dag.Transformation<?>> getTransitivePredecessorsInternal() {
                return List.of(this);
            }

            @Override
            public List<org.apache.flink.api.dag.Transformation<?>> getInputs() {
                return List.of();
            }
        };
    }
}
