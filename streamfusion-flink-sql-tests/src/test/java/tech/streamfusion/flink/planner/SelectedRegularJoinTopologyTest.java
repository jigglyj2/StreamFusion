/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Ordinary default SQL selection must create an Arrow join/Calc owner behind two IPC edges. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SelectedRegularJoinTopologyTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void defaultJoinAndProjectionShareOneNativeOwner(int parallelism) throws Exception {
        var previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            var tables = StreamTableEnvironment.create(env);
            assertThat(tables.getConfig().get(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED))
                    .isFalse();
            tables.executeSql(
                    "CREATE TABLE left_input (k BIGINT, v BIGINT) WITH ('connector'='datagen','number-of-rows'='1')");
            tables.executeSql(
                    "CREATE TABLE right_input (k BIGINT, v BIGINT) WITH ('connector'='datagen','number-of-rows'='1')");
            var output = tables.toChangelogStream(
                    tables.sqlQuery("SELECT a.k,b.v FROM left_input a JOIN right_input b ON a.k=b.k"));
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            var owners = output.getTransformation().getTransitivePredecessors().stream()
                    .filter(node -> node instanceof KeyedMultipleInputTransformation)
                    .map(node -> (KeyedMultipleInputTransformation<?>) node)
                    .collect(java.util.stream.Collectors.toList());
            assertThat(owners).hasSize(1);
            var owner = owners.get(0);
            assertThat(owner.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(owner.getInputs()).hasSize(2).allSatisfy(input -> assertThat(input.getOutputType())
                    .isInstanceOf(NativeExchangeFrameTypeInfo.class));
            assertThat(owner.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
            var factory = owner.getOperatorFactory();
            var plan = NativePlan.parseFrom((byte[]) field(factory, "plan"));
            assertThat(plan.getRoot().hasCalc()).isTrue();
            var join = plan.getRoot().getCalc().getInput();
            assertThat(join.hasRegularJoin()).isTrue();
            assertThat(join.getPlanNodeId())
                    .isPositive()
                    .isNotEqualTo(plan.getRoot().getPlanNodeId());
            assertThat(field(factory, "stateIds")).isEqualTo(List.of(join.getPlanNodeId()));
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    private static Object field(Object value, String name) throws Exception {
        var field = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(value);
    }
}
