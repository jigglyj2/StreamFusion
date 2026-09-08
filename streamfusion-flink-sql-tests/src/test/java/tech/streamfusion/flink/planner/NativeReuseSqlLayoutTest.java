/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** SQL-generated reuse stays in one owner without changing Flink's selected exchanges. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class NativeReuseSqlLayoutTest {
    @Test
    void reusedHopCountAndAttachedMaxHaveOneOwnerWithTwoExits() throws Exception {
        var factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        var processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            System.setProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, Capture.class.getName());
            for (int size : List.of(6, 10)) {
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(2);
                var source = env.fromCollection(
                        List.of(Row.of(1L, LocalDateTime.of(2026, 1, 1, 0, 0))),
                        Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME));
                var tables = StreamTableEnvironment.create(env);
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
                tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
                tables.getConfig()
                        .set(
                                OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                                AggregatePhaseStrategy.TWO_PHASE);
                tables.createTemporaryView(
                        "events",
                        source,
                        Schema.newBuilder()
                                .column("k", DataTypes.BIGINT())
                                .column("ts", DataTypes.TIMESTAMP(3).notNull())
                                .watermark("ts", "ts")
                                .build());
                tables.toChangelogStream(
                        tables.sqlQuery("WITH counts AS (SELECT k, COUNT(*) n, window_start s, window_end e "
                                + "FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '" + size
                                + "' SECOND)) "
                                + "GROUP BY k, window_start, window_end) "
                                + "SELECT a.k, a.n FROM counts a JOIN (SELECT MAX(n) m, s, e FROM counts GROUP BY s, e) b "
                                + "ON a.s=b.s AND a.e=b.e AND a.n>=b.m"));
                var layout = Capture.layout;
                layout.validateBoundaryGraph();
                var owners = layout.regions.stream()
                        .filter(region -> region.outputs.size() > 1)
                        .collect(java.util.stream.Collectors.toList());
                assertThat(owners).hasSize(1);
                var owner = owners.get(0);
                assertThat(owner.stages)
                        .extracting(node -> node.getClass().getSimpleName())
                        .containsExactly(
                                "StreamExecGlobalWindowAggregate", "StreamExecCalc", "StreamExecLocalWindowAggregate");
                assertThat(owner.inputs).hasSize(1);
                assertThat(owner.inputs.get(0).getClass().getSimpleName()).isEqualTo("StreamExecExchange");
                assertThat(owner.outputs).containsExactly(owner.stages.get(0), owner.stages.get(2));
                assertThat(layout.sharedInternalStages().keySet()).containsExactly(owner.stages.get(0));
                assertThat(owner.stageInputs.get(1).get(0).index).isZero();
                assertThat(owner.stageInputs.get(1).get(0).external).isFalse();
                var contracts = new java.util.ArrayList<tech.streamfusion.proto.plan.v1.NativeRegionPlan>();
                for (var bytes : Capture.plans)
                    contracts.add(tech.streamfusion.proto.plan.v1.NativeRegionPlan.parseFrom(bytes));
                var sharedPlans = contracts.stream()
                        .filter(plan -> plan.getOutputStageIdsCount() == 2)
                        .collect(java.util.stream.Collectors.toList());
                assertThat(sharedPlans).hasSize(1);
                var contract = sharedPlans.get(0);
                assertThat(contract.getStagesCount()).isEqualTo(3);
                assertThat(contract.getInputCount()).isEqualTo(1);
                long sharedId = (1L << 32) | owner.stages.get(0).getId();
                assertThat(contract.getStages(0).getOperator().getPlanNodeId()).isEqualTo(sharedId);
                assertThat(contract.getStages(1).getInputs(0).getStageId()).isEqualTo(sharedId);
                assertThat(contract.getOutputStageIds(0)).isEqualTo(sharedId);
                assertThat(contract.getStages(0).getOperator().hasWindowAggregate())
                        .isTrue();
                assertThat(contract.getStages(2).getOperator().hasLocalWindowAggregate())
                        .isTrue();

                // Inspection did not replace or translate a second copy of the original operator.
                assertThat(((ExecNodeBase<?>) owner.stages.get(0)).getTransformation())
                        .isNotNull();
            }
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    public static final class Capture implements ExecNodeGraphProcessor {
        static StreamFusionNativeRegionLayout layout;
        static List<byte[]> plans;

        @Override
        public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
            var names = Set.of(
                    "StreamExecCalc",
                    "StreamExecLocalWindowAggregate",
                    "StreamExecGlobalWindowAggregate",
                    "StreamExecMultiJoin",
                    "StreamExecJoin");
            layout = StreamFusionNativeRegionLayout.discover(
                    graph, node -> names.contains(node.getClass().getSimpleName()));
            // The test-only conversion helper commits retained source/sink boundary edges.
            // Snapshot those edges so contract inspection does not accelerate this Flink run.
            var edges = new java.util.IdentityHashMap<
                    org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?>,
                    List<org.apache.flink.table.planner.plan.nodes.exec.ExecEdge>>();
            var pending = new java.util.ArrayList<>(graph.getRootNodes());
            while (!pending.isEmpty()) {
                var node = pending.remove(pending.size() - 1);
                if (edges.putIfAbsent(node, List.copyOf(node.getInputEdges())) != null) continue;
                for (var edge : node.getInputEdges()) pending.add(edge.getSource());
            }
            try {
                var selected = new StreamFusionExecGraphProcessor()
                        .convert(graph.getRootNodes().get(0));
                var selectedLayout = StreamFusionNativeRegionLayout.discover(
                        new ExecNodeGraph(List.of(selected)), node -> node instanceof StreamFusionNativePlanNode);
                selectedLayout.validateBoundaryGraph();
                plans = selectedLayout.regions.stream()
                        .map(region -> region.plan(context.getPlanner()))
                        .collect(java.util.stream.Collectors.toList());
            } finally {
                edges.forEach((node, originalEdges) -> node.setInputEdges(originalEdges));
            }
            return graph;
        }
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
