/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionTranslator;
import tech.streamfusion.flink.planner.join.StreamFusionRegularJoinTranslator;

/** Bare and composed joins, with no trailing Calc to repair their record envelopes. */
final class SharedJoinChainFixture {
    static final RowType INPUT = RowType.of(new BigIntType(false), new VarCharType());
    final int stages;
    final RowType output;
    final List<RowType> inputs;
    private final Configuration config = new Configuration();

    SharedJoinChainFixture(boolean chain) {
        stages = chain ? 2 : 1;
        output = outputType(stages - 1);
        inputs = java.util.Collections.nCopies(stages + 1, INPUT);
    }

    static RowType outputType(int stage) {
        return RowType.of(java.util.stream.IntStream.range(0, (stage + 2) * INPUT.getFieldCount())
                .mapToObj(index -> INPUT.getTypeAt(index % INPUT.getFieldCount()))
                .toArray(org.apache.flink.table.types.logical.LogicalType[]::new));
    }

    static long id(int stage) {
        return (1L << 32) | (stage + 1);
    }

    static String name(int stage) {
        return "join-chain-stage-" + stage;
    }

    static String uid(int stage) {
        return name(stage) + "-uid";
    }

    static OperatorID operatorId(int stage) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(uid(stage)));
    }

    List<Long> stateIds() {
        return java.util.stream.IntStream.range(0, stages)
                .mapToObj(SharedJoinChainFixture::id)
                .collect(java.util.stream.Collectors.toList());
    }

    byte[] plan() {
        byte[] left = StreamFusionNativeRegionTranslator.inputPlan(0);
        for (int stage = 0; stage < stages; stage++) {
            RowType leftType = stage == 0 ? INPUT : outputType(stage - 1);
            byte[] join = StreamFusionRegularJoinTranslator.createStagePlan(
                    leftType,
                    INPUT,
                    outputType(stage),
                    new JoinSpec(FlinkJoinType.INNER, new int[] {0}, new int[] {0}, new boolean[] {true}, null),
                    List.of(),
                    List.of(),
                    0,
                    0,
                    config);
            join = StreamFusionNativeRegionTranslator.identifyStage(join, stage + 1, name(stage), uid(stage));
            left = StreamFusionNativeRegionTranslator.composeWithInputs(
                    join, List.of(left, StreamFusionNativeRegionTranslator.inputPlan(stage + 1)));
        }
        return left;
    }

    static GenericRowData row(long key, int port) {
        return GenericRowData.of(key, key % 3 == 0 ? null : StringData.fromString("漢😀é-" + port + "-" + key));
    }

    Oracle oracle(boolean rocks) throws Exception {
        return new Oracle(rocks);
    }

    final class Oracle implements AutoCloseable {
        final List<FlinkRegularJoinMetricOracle> joins = new ArrayList<>();
        private final List<StreamElement> pending = new ArrayList<>();

        Oracle(boolean rocks) throws Exception {
            var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
            var rex = new RexBuilder(types);
            for (int stage = 0; stage < stages; stage++) {
                RowType left = stage == 0 ? INPUT : outputType(stage - 1);
                var key = types.createFieldTypeFromLogicalType(INPUT.getTypeAt(0));
                var equality = rex.makeCall(
                        org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS,
                        rex.makeInputRef(key, 0),
                        rex.makeInputRef(key, left.getFieldCount()));
                var condition = org.apache.flink.table.planner.plan.utils.JoinUtil.generateConditionFunction(
                        config, getClass().getClassLoader(), equality, left, INPUT);
                joins.add(RegularJoinFlinkHarness.create(
                        left, INPUT, outputType(stage), condition, rocks, operatorId(stage), name(stage)));
            }
        }

        void accept(int port, StreamElement event) throws Exception {
            int first = port < 2 ? 0 : port - 1;
            joins.get(first).accept(port == 0 ? 0 : 1, event);
            List<StreamElement> events = joins.get(first).drain();
            for (int stage = first + 1; stage < stages; stage++) {
                for (var value : events) joins.get(stage).accept(0, value);
                events = joins.get(stage).drain();
            }
            pending.addAll(events);
        }

        List<StreamElement> drain() {
            var result = List.copyOf(pending);
            pending.clear();
            return result;
        }

        void preBarrier(long checkpoint) throws Exception {
            for (var join : joins) join.prepareSnapshotPreBarrier(checkpoint);
        }

        @Override
        public void close() throws Exception {
            org.apache.flink.util.IOUtils.closeAll(joins);
        }
    }
}
