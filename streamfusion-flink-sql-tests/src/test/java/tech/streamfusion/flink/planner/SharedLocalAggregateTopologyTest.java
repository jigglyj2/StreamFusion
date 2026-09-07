/* Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.*;
import static tech.streamfusion.flink.planner.LocalAggregateFixtures.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.aggregate.StreamFusionLocalGroupAggregateTranslator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.proto.plan.v1.NativePlan;

class SharedLocalAggregateTopologyTest {
    @Test
    void selectedLocalUsesCommonUnaryArrowOwnerAndEffectiveConfiguration() throws Exception {
        var environment = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        var planner = (PlannerBase) environment.getPlanner();
        environment.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        environment.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 11L);
        for (Configuration config : List.of(new Configuration(), config(7))) {
            var source = new NativeRegionTopologyTest.ArrowSource(SharedAggregateFlinkOracle.INPUT);
            var local = new StreamFusionExecLocalGroupAggregate(
                    config,
                    new int[] {0},
                    calls(),
                    new boolean[] {true, true, true, true},
                    true,
                    InputProperty.DEFAULT,
                    GlobalPartialFixtures.PARTIAL);
            local.setInputEdges(
                    List.of(ExecEdge.builder().source(source).target(local).build()));
            assertThat(local.ownsNativeKeyedState()).isFalse();
            var result = (OneInputTransformation<?, ?>) local.translateToPlan(planner);
            assertThat(result.getOperator()).isInstanceOf(StreamFusionArrowNativeOperator.class);
            assertThat(result.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(result.getInputs().get(0).getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(source.translations).isOne();
            var field = StreamFusionArrowNativeOperator.class.getDeclaredField("serializedPlan");
            field.setAccessible(true);
            var plan = NativePlan.parseFrom((byte[]) field.get(result.getOperator()));
            assertThat(plan.getProtocolVersion()).isEqualTo(2);
            assertThat(plan.getRoot().getPlanNodeId()).isEqualTo((1L << 32) | local.getId());
            assertThat(plan.getRoot().getLocalGroupAggregate().getMiniBatchSize())
                    .isEqualTo(config.toMap().isEmpty() ? 11 : 7);
            assertThat(plan.getRoot().getLocalGroupAggregate().getInput().hasInput())
                    .isTrue();
        }
    }

    @Test
    void malformedFragmentsRejectBeforeRuntimeCreation() {
        assertThat(reason(config(0), GlobalPartialFixtures.PARTIAL, new boolean[] {true, true, true, true}))
                .contains("positive");
        assertThat(reason(new Configuration(), GlobalPartialFixtures.PARTIAL, new boolean[] {true, true, true, true}))
                .contains("enabled");
        assertThat(reason(config(7), GlobalPartialFixtures.PARTIAL, new boolean[] {true, false, true, true}))
                .contains("retractable");
        assertThat(reason(config(7), GlobalPartialFixtures.PARTIAL, new boolean[] {true}))
                .contains("equally sized");
        var nullable = RowType.of(GlobalPartialFixtures.PARTIAL.getTypeAt(0), new VarBinaryType());
        assertThat(reason(config(7), nullable, new boolean[] {true, true, true, true}))
                .contains("non-null VARBINARY");
    }

    private static String reason(Configuration config, RowType output, boolean[] retractions) {
        return StreamFusionLocalGroupAggregateTranslator.unsupportedStageReason(
                SharedAggregateFlinkOracle.INPUT, output, new int[] {0}, calls(), retractions, true, config);
    }
}
