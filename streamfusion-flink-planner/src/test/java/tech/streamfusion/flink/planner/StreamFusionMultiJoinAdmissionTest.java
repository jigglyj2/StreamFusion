/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/** Binary lowering changes the storage algorithm, not the remaining production requirements. */
class StreamFusionMultiJoinAdmissionTest {
    private final Configuration config = new Configuration();
    private final RowType inputType = RowType.of(new BigIntType(false));
    private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
    private final RexBuilder rex = new RexBuilder(types);

    @Test
    void binaryJoinComposesWithCalcWhileRetainingItsPersistentStateGate() {
        var join = join(2, true);
        var root = new StreamExecCalc(config, List.of(ref(0)), null, InputProperty.DEFAULT, inputType, "projection");
        root.setInputEdges(List.of(ExecEdge.builder().source(join).target(root).build()));
        var graph = new ExecNodeGraph(List.of(root));
        var original = root.getInputEdges().get(0);
        var reasons = new ArrayList<String>();
        StreamFusionArchitectureSupport.collect(graph, reasons);
        assertThat(reasons).hasSize(1);
        assertThat(String.join("\n", reasons))
                .contains("retained-state/buffer admission")
                .doesNotContain("fused native ExecutionPlan")
                .doesNotContain("whole-key", "dirty-page", "fan-out is not yet drained");
        assertThat(new StreamFusionExecGraphProcessor().process(graph, null)).isSameAs(graph);
        assertThat(root.getInputEdges()).containsExactly(original);
    }

    @Test
    void genuineMultiWayAndNonLowerableBinaryShapesKeepIntegrationGate() {
        for (var join : List.of(join(3, true), join(2, false))) {
            var reasons = new ArrayList<String>();
            StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(join)), reasons);
            assertThat(reasons).hasSize(2);
            assertThat(String.join("\n", reasons))
                    .contains(
                            "retained-state/buffer admission",
                            "paged state and bounded output cursor",
                            "checkpoint/control")
                    .doesNotContain("rewrites whole-key", "fan-out is not yet drained");
        }
    }

    private StreamExecMultiJoin join(int inputs, boolean equiKeys) {
        return join(inputs, equiKeys, FlinkJoinType.INNER);
    }

    @Test
    void outerBinaryMultiJoinMustNotBorrowRegularJoinsChangelogContract() {
        var join = join(2, true, FlinkJoinType.LEFT);
        assertThat(FlinkExecNodeAccess.binaryMultiJoinSpec(join)).isNull();
        assertThat(new StreamFusionExecGraphProcessor().convert(join)).isInstanceOf(StreamFusionExecMultiJoin.class);
        var reasons = new ArrayList<String>();
        StreamFusionArchitectureSupport.collect(new ExecNodeGraph(List.of(join)), reasons);
        assertThat(String.join("\n", reasons)).contains("checkpoint/control lifecycle");
    }

    private StreamExecMultiJoin join(int inputs, boolean equiKeys, FlinkJoinType type) {
        var joinTypes = new ArrayList<FlinkJoinType>();
        var conditions = new ArrayList<RexNode>();
        var properties = new ArrayList<InputProperty>();
        var uniqueKeys = new ArrayList<List<int[]>>();
        var attributes = new java.util.HashMap<Integer, List<ConditionAttributeRef>>();
        for (int index = 0; index < inputs; index++) {
            joinTypes.add(index == 0 ? FlinkJoinType.INNER : type);
            conditions.add(index == 0 ? null : rex.makeCall(SqlStdOperatorTable.EQUALS, ref(0), ref(index)));
            properties.add(InputProperty.DEFAULT);
            uniqueKeys.add(List.of());
            if (index > 0 && equiKeys) attributes.put(index, List.of(new ConditionAttributeRef(0, 0, index, 0)));
        }
        var fields = new org.apache.flink.table.types.logical.LogicalType[inputs];
        java.util.Arrays.fill(fields, new BigIntType(false));
        var join = new StreamExecMultiJoin(
                config,
                joinTypes,
                conditions,
                conditions.get(inputs - 1),
                attributes,
                uniqueKeys,
                Map.of(),
                properties,
                RowType.of(fields),
                "join");
        var edges = new ArrayList<ExecEdge>();
        for (int index = 0; index < inputs; index++) {
            var source = new StreamExecTableSourceScan(config, null, inputType, "input " + index);
            source.setInputEdges(List.of());
            edges.add(ExecEdge.builder().source(source).target(join).build());
        }
        join.setInputEdges(edges);
        return join;
    }

    private RexNode ref(int index) {
        return rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), index);
    }
}
