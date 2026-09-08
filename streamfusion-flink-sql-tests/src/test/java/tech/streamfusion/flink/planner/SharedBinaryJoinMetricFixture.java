/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.join.StreamFusionRegularJoinTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;

/** Q3's binary MultiJoin shape, using Flink's actual multi-input operator and generated Calc. */
final class SharedBinaryJoinMetricFixture {
    static final RowType INPUT = RowType.of(new BigIntType(false), new VarCharType());
    static final RowType OUTPUT = RowType.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType());
    private final Configuration config = new Configuration();
    private final boolean outer;
    final RowType input;
    final RowType output;
    private final boolean range;
    private final RexNode condition;
    private final List<RexNode> projections;

    SharedBinaryJoinMetricFixture(boolean outer) {
        this(outer, INPUT, OUTPUT, false);
    }

    static SharedBinaryJoinMetricFixture rangeJoin() {
        var input = RowType.of(
                new BigIntType(false),
                new org.apache.flink.table.types.logical.TimestampType(3),
                new org.apache.flink.table.types.logical.TimestampType(3));
        var output = RowType.of(
                input.getTypeAt(0),
                input.getTypeAt(1),
                input.getTypeAt(2),
                input.getTypeAt(0),
                input.getTypeAt(1),
                input.getTypeAt(2));
        return new SharedBinaryJoinMetricFixture(false, input, output, true);
    }

    private SharedBinaryJoinMetricFixture(boolean outer, RowType input, RowType output, boolean range) {
        this.outer = outer;
        this.input = input;
        this.output = output;
        this.range = range;
        var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        var rex = new RexBuilder(types);
        int width = input.getFieldCount();
        projections = java.util.stream.IntStream.range(0, output.getFieldCount())
                .map(index -> (index + width) % output.getFieldCount())
                .mapToObj(index -> (RexNode)
                        rex.makeInputRef(types.createFieldTypeFromLogicalType(output.getTypeAt(index)), index))
                .collect(java.util.stream.Collectors.toList());
        if (range) {
            var timestamp = types.createFieldTypeFromLogicalType(input.getTypeAt(1));
            var value = rex.makeInputRef(timestamp, width + 1);
            condition = rex.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.AND,
                    rex.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                            value,
                            rex.makeInputRef(timestamp, 1)),
                    rex.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                            value,
                            rex.makeInputRef(timestamp, 2)));
        } else condition = null;
    }

    org.apache.flink.table.data.GenericRowData row(long key, int port) {
        if (!range)
            return org.apache.flink.table.data.GenericRowData.of(
                    key,
                    key % 7 == 0 ? null : org.apache.flink.table.data.StringData.fromString("é-" + port + "-" + key));
        long start = key * 1000 - 32000;
        // Null, below range, inclusive boundaries, inside and above range; negative epochs too.
        Long value = start;
        if (port != 0)
            value = key % 6 == 0 ? null : Long.valueOf(start + new long[] {0, 0, 5, 10, -1, 11}[(int) (key % 6)]);
        return org.apache.flink.table.data.GenericRowData.of(
                key,
                value == null ? null : org.apache.flink.table.data.TimestampData.fromEpochMillis(value),
                key % 11 == 0 ? null : org.apache.flink.table.data.TimestampData.fromEpochMillis(start + 10));
    }

    static long id(int stage) {
        return (1L << 32) | (stage + 1);
    }

    static String name(int stage) {
        return "binary-join-conformance-" + stage;
    }

    static String uid(int stage) {
        return name(stage) + "-uid";
    }

    static OperatorID operatorId(int stage) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(uid(stage)));
    }

    byte[] plan() {
        byte[] join = StreamFusionRegularJoinTranslator.createStagePlan(
                input,
                input,
                output,
                new JoinSpec(
                        outer ? FlinkJoinType.LEFT : FlinkJoinType.INNER,
                        new int[] {0},
                        new int[] {0},
                        new boolean[] {true},
                        condition),
                List.of(),
                List.of(),
                0,
                0,
                config);
        join = StreamFusionNativeRegionTranslator.identifyStage(join, 1, name(0), uid(0));
        join = StreamFusionNativeRegionTranslator.composeWithInputs(
                join,
                List.of(
                        StreamFusionNativeRegionTranslator.inputPlan(0),
                        StreamFusionNativeRegionTranslator.inputPlan(1)));
        byte[] calc = StreamFusionCalcTranslator.createStagePlan(output, output, projections, null);
        calc = StreamFusionNativeRegionTranslator.identifyStage(calc, 2, name(1), uid(1));
        return StreamFusionNativeRegionTranslator.composeWithInputs(calc, List.of(join));
    }

    FlinkMultiInputMetricOracle join(boolean rocks) throws Exception {
        var attributes = Map.of(1, List.of(new ConditionAttributeRef(0, 0, 1, 0)));
        var extractor = new AttributeBasedJoinKeyExtractor(attributes, List.of(input, input));
        String code =
                "public class MetricBinaryJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.JoinCondition { public MetricBinaryJoinCondition(Object[] refs) {} "
                        + "public boolean apply(org.apache.flink.table.data.RowData left, org.apache.flink.table.data.RowData right) "
                        + "{ return left.getLong(0) == right.getLong(0)"
                        + (range
                                ? " && !left.isNullAt(1) && !left.isNullAt(2) && !right.isNullAt(1)"
                                        + " && right.getTimestamp(1, 3).compareTo(left.getTimestamp(1, 3)) >= 0"
                                        + " && right.getTimestamp(1, 3).compareTo(left.getTimestamp(2, 3)) <= 0"
                                : "")
                        + "; }}";
        var factory = new StreamingMultiJoinOperatorFactory(
                List.of(InternalTypeInfo.of(input), InternalTypeInfo.of(input)),
                List.of(JoinInputSideSpec.withoutUniqueKey(), JoinInputSideSpec.withoutUniqueKey()),
                List.of(FlinkJoinType.INNER, outer ? FlinkJoinType.LEFT : FlinkJoinType.INNER),
                null,
                new long[] {0, 0},
                new GeneratedJoinCondition[] {
                    null, new GeneratedJoinCondition("MetricBinaryJoinCondition", code, new Object[0])
                },
                extractor,
                attributes);
        var keys = KeySelectorUtil.getRowDataSelector(
                getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(input));
        var harness = new FlinkMultiInputMetricOracle.Harness(factory);
        harness.getStreamConfig()
                .setStateKeySerializer(keys.getProducedType()
                        .createSerializer(new org.apache.flink.api.common.serialization.SerializerConfigImpl()));
        harness.setKeySelector(0, keys);
        harness.setKeySelector(1, keys);
        harness.setStateBackend(
                rocks
                        ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                        : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        harness.getStreamConfig().setOperatorID(operatorId(0));
        harness.getStreamConfig().setOperatorName(name(0));
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return new FlinkMultiInputMetricOracle(harness, 2);
    }

    FlinkStageMetricOracle calc() throws Exception {
        var transformation = new Transformation<RowData>("input", InternalTypeInfo.of(output), 1) {
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
                transformation,
                output,
                scala.collection.JavaConverters.asScalaBufferConverter(projections)
                        .asScala()
                        .toSeq(),
                scala.Option.empty(),
                true,
                "JoinMetricCalc");
        var harness = new OneInputStreamOperatorTestHarness<RowData, RowData>(factory, 16, 1, 0);
        harness.getStreamConfig().setOperatorID(operatorId(1));
        harness.getStreamConfig().setOperatorName(name(1));
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return new FlinkStageMetricOracle(harness);
    }
}
