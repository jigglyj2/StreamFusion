/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateTranslator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;

/** Operator-specific semantics only; tree composition, runtime and metric discovery are shared. */
final class SharedDeduplicateMetricFixture {
    static final RowType TYPE =
            RowType.of(new VarCharType(), new TimestampType(false, TimestampKind.ROWTIME, 3), new BigIntType());
    final boolean rowtime;
    final boolean keepLast;
    final boolean before;
    final boolean insert;
    final Configuration config = new Configuration();
    private final FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    private final RexBuilder rex = new RexBuilder(types);
    private final List<RexNode> projections = new ArrayList<>();

    SharedDeduplicateMetricFixture(boolean rowtime, boolean keepLast, boolean before, boolean insert) {
        this.rowtime = rowtime;
        this.keepLast = keepLast;
        this.before = before;
        this.insert = insert;
        config.set(ExecutionConfigOptions.TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED, insert);
        for (int field = 0; field < TYPE.getFieldCount(); field++)
            projections.add(rex.makeInputRef(types.createFieldTypeFromLogicalType(TYPE.getTypeAt(field)), field));
    }

    static long id(int stage) {
        return (1L << 32) | (stage + 1);
    }

    static String name(int stage) {
        return "dedup-conformance-" + stage;
    }

    static String uid(int stage) {
        return "dedup-conformance-uid-" + stage;
    }

    static OperatorID operatorId(int stage) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(uid(stage)));
    }

    private RexNode condition(int stage) {
        return rex.makeCall(
                stage == 0 ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN,
                projections.get(2),
                rex.makeExactLiteral(BigDecimal.valueOf(stage == 0 ? 0 : 9)));
    }

    byte[] plan() {
        var fragments = new ArrayList<byte[]>();
        for (int stage = 0; stage < 3; stage++) {
            byte[] plan = stage == 1
                    ? StreamFusionDeduplicateTranslator.createStagePlan(
                            TYPE, TYPE, new int[] {0}, rowtime, keepLast, false, before, 0, config)
                    : StreamFusionCalcTranslator.createStagePlan(TYPE, TYPE, projections, condition(stage));
            fragments.add(StreamFusionNativeRegionTranslator.identifyStage(plan, stage + 1, name(stage), uid(stage)));
        }
        return StreamFusionNativeRegionTranslator.compose(fragments);
    }

    FlinkStageMetricOracle oracle(int stage, boolean rocks) throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> harness;
        if (stage == 1) {
            var info = InternalTypeInfo.<RowData>of(TYPE);
            var keys = KeySelectorUtil.getRowDataSelector(getClass().getClassLoader(), new int[] {0}, info);
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<RowData, RowData, RowData> function;
            if (rowtime) function = new RowTimeDeduplicateFunction(info, 0, 1, before, insert, keepLast);
            else if (!keepLast) function = new ProcTimeDeduplicateKeepFirstRowFunction(0);
            else
                function = new ProcTimeDeduplicateKeepLastRowFunction(
                        info,
                        0,
                        before,
                        insert,
                        true,
                        new EqualiserCodeGenerator(TYPE, getClass().getClassLoader())
                                .generateRecordEqualiser("DedupConformanceEqualiser"),
                        null);
            harness = new KeyedOneInputStreamOperatorTestHarness<>(
                    new KeyedProcessOperator<>(function), keys, keys.getProducedType(), 16, 1, 0);
            harness.setStateBackend(
                    rocks
                            ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                            : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        } else {
            var factory = CalcCodeGenerator.generateCalcOperator(
                    new CodeGeneratorContext(config, getClass().getClassLoader()),
                    input(),
                    TYPE,
                    scala.collection.JavaConverters.asScalaBufferConverter(projections)
                            .asScala()
                            .toSeq(),
                    scala.Option.apply(condition(stage)),
                    true,
                    "DedupConformanceCalc" + stage);
            harness = new OneInputStreamOperatorTestHarness<>(factory, 16, 1, 0);
        }
        harness.getStreamConfig().setOperatorID(operatorId(stage));
        harness.getStreamConfig().setOperatorName(name(stage));
        harness.setup(new RowDataSerializer(TYPE));
        harness.open();
        return new FlinkStageMetricOracle(harness);
    }

    private static Transformation<RowData> input() {
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
