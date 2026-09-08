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
    void reusedHopCountAndAttachedMaxHaveOneOwnerWithTwoExits() {
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
            return graph;
        }
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
