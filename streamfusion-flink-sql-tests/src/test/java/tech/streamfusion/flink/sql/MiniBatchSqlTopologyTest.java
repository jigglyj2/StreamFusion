/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Ordinary SQL must retain an Arrow control stage and one shared native state owner. */
class MiniBatchSqlTopologyTest extends SqlParityTestSupport {
    @Test
    void ordinarySelectionRetainsConfiguredBundlesAndArrowControlBoundaries() throws Exception {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        for (int parallelism : List.of(1, 3)) {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            var tables = StreamTableEnvironment.create(env);
            tables.getConfig()
                    .set(
                            ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION,
                            parallelism == 1
                                    ? ExecutionConfigOptions.UidGeneration.DISABLED
                                    : ExecutionConfigOptions.UidGeneration.ALWAYS);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
            tables.getConfig().setIdleStateRetention(Duration.ZERO);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 7L);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(5));
            tables.getConfig()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.ONE_PHASE);
            var input = env.fromCollection(
                    List.of(Row.of("é", 1L)), Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG));
            tables.createTemporaryView(
                    "mini_topology",
                    tables.fromChangelogStream(input, Schema.newBuilder().build()));
            var output = tables.toChangelogStream(
                    tables.sqlQuery("SELECT k, COUNT(*) + 1 AS n, SUM(v) AS s FROM mini_topology GROUP BY k"));
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            int owners = 0;
            int assigners = 0;
            for (var transformation : output.getTransformation().getTransitivePredecessors()) {
                if (transformation instanceof KeyedMultipleInputTransformation) {
                    owners++;
                    var owner = (KeyedMultipleInputTransformation<?>) transformation;
                    assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
                    assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
                    assertThat(owner.getInputs()).allSatisfy(edge -> assertThat(edge.getOutputType())
                            .isInstanceOf(NativeExchangeFrameTypeInfo.class));
                    var field = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("plan");
                    field.setAccessible(true);
                    var plan = NativePlan.parseFrom((byte[]) field.get(owner.getOperatorFactory()));
                    assertThat(plan.getRoot().hasCalc()).isTrue();
                    var aggregate = plan.getRoot().getCalc().getInput();
                    assertThat(aggregate.hasGroupAggregate()).isTrue();
                    assertThat(aggregate.getGroupAggregate().getMiniBatchSize()).isEqualTo(7);
                    assertThat(aggregate.getMetricName()).contains("GroupAggregate");
                    if (parallelism == 1) assertThat(aggregate.getMetricUid()).isEmpty();
                    else assertThat(aggregate.getMetricUid()).endsWith("_group-aggregate");
                }
                if (transformation instanceof OneInputTransformation) {
                    var unary = (OneInputTransformation<?, ?>) transformation;
                    if (!(unary.getOperatorFactory() instanceof SimpleOperatorFactory)) continue;
                    String name = unary.getOperator().getClass().getName();
                    if (name.endsWith("StreamFusionArrowProcTimeMiniBatchAssignerOperator")) assigners++;
                    if (name.startsWith("tech.streamfusion.") && !name.endsWith("ArrowBatchToRowDataOperator"))
                        assertThat(unary.getOutputType())
                                .satisfiesAnyOf(
                                        type -> assertThat(type).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE),
                                        type -> assertThat(type).isInstanceOf(NativeExchangeFrameTypeInfo.class));
                }
            }
            assertThat(owners).isOne();
            assertThat(assigners).isOne();
        }
    }
}
