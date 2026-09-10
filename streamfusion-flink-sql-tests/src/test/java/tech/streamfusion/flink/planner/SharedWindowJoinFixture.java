/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.time.Duration;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.flink.planner.window.StreamFusionWindowJoinTranslator;

/** Attached-window join followed by generated projection, sharing production plan builders. */
final class SharedWindowJoinFixture {
    static final RowType INPUT =
            RowType.of(new BigIntType(), new BigIntType(false), new BigIntType(), new VarCharType());
    static final RowType OUTPUT = RowType.of(java.util.stream.IntStream.range(0, 8)
            .mapToObj(i -> INPUT.getTypeAt(i % 4))
            .toArray(LogicalType[]::new));
    final int[] keys;
    private final Configuration config = new Configuration();
    private final JoinSpec spec;
    private final List<RexNode> projections;

    SharedWindowJoinFixture(boolean keyed, boolean residual) {
        keys = keyed ? new int[] {0} : new int[0];
        var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        RexNode condition = residual
                ? rex.makeCall(
                        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        rex.makeInputRef(types.createFieldTypeFromLogicalType(INPUT.getTypeAt(2)), 2),
                        rex.makeInputRef(types.createFieldTypeFromLogicalType(INPUT.getTypeAt(2)), 6))
                : null;
        spec = new JoinSpec(FlinkJoinType.INNER, keys, keys, keyed ? new boolean[] {true} : new boolean[0], condition);
        projections = java.util.stream.IntStream.range(0, 8)
                .map(i -> (i + 4) % 8)
                .mapToObj(i -> (RexNode) rex.makeInputRef(types.createFieldTypeFromLogicalType(OUTPUT.getTypeAt(i)), i))
                .collect(java.util.stream.Collectors.toList());
    }

    static long id(int stage) {
        return (1L << 32) | (1001 + stage);
    }

    static String name(int stage) {
        return "window-join-conformance-" + stage;
    }

    static String uid(int stage) {
        return name(stage) + "-uid";
    }

    static OperatorID operatorId(int stage) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(uid(stage)));
    }

    byte[] plan() {
        var window = new WindowAttachedWindowingStrategy(
                new TumblingWindowSpec(Duration.ofSeconds(1), null),
                new TimestampType(false, TimestampKind.ROWTIME, 3),
                1);
        byte[] join =
                StreamFusionWindowJoinTranslator.createStagePlan(INPUT, INPUT, OUTPUT, spec, window, window, config);
        join = StreamFusionNativeRegionTranslator.identifyStage(join, 1001, name(0), uid(0));
        join = StreamFusionNativeRegionTranslator.composeWithInputs(
                join,
                List.of(
                        StreamFusionNativeRegionTranslator.inputPlan(0),
                        StreamFusionNativeRegionTranslator.inputPlan(1)));
        byte[] calc = StreamFusionCalcTranslator.createStagePlan(OUTPUT, OUTPUT, projections, null);
        calc = StreamFusionNativeRegionTranslator.identifyStage(calc, 1002, name(1), uid(1));
        return StreamFusionNativeRegionTranslator.composeWithInputs(calc, List.of(join));
    }

    FlinkRegularJoinMetricOracle join(boolean rocks) throws Exception {
        var condition = JoinUtil.generateConditionFunction(config, getClass().getClassLoader(), spec, INPUT, INPUT);
        var operator = WindowJoinOperatorBuilder.builder()
                .leftSerializer(new RowDataSerializer(INPUT))
                .rightSerializer(new RowDataSerializer(INPUT))
                .generatedJoinCondition(condition)
                .leftWindowEndIndex(1)
                .rightWindowEndIndex(1)
                .filterNullKeys(spec.getFilterNulls())
                .joinType(FlinkJoinType.INNER)
                .build();
        var selector =
                KeySelectorUtil.getRowDataSelector(getClass().getClassLoader(), keys, InternalTypeInfo.of(INPUT));
        var harness = new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
                operator, selector, selector, selector.getProducedType(), 16, 1, 0);
        harness.setStateBackend(
                rocks
                        ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                        : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        harness.getStreamConfig().setOperatorID(operatorId(0));
        harness.getStreamConfig().setOperatorName(name(0));
        harness.setup(new RowDataSerializer(OUTPUT));
        harness.open();
        return new FlinkRegularJoinMetricOracle(harness);
    }

    FlinkStageMetricOracle calc() throws Exception {
        var input = new Transformation<RowData>("input", InternalTypeInfo.of(OUTPUT), 1) {
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
                new CodeGeneratorContext(config, getClass().getClassLoader()),
                input,
                OUTPUT,
                scala.collection.JavaConverters.asScalaBufferConverter(projections)
                        .asScala()
                        .toSeq(),
                scala.Option.empty(),
                true,
                "WindowJoinMetricCalc");
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 16, 1, 0);
        harness.getStreamConfig().setOperatorID(operatorId(1));
        harness.getStreamConfig().setOperatorName(name(1));
        harness.setup(new RowDataSerializer(OUTPUT));
        harness.open();
        return new FlinkStageMetricOracle(harness);
    }
}
