/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateTranslator;

/** A new selected state-owner class needs no entries in region discovery/composition dispatch. */
class StatefulNativeFragmentContractTest {
    @Test
    void unknownStateOwnersOnSeparateBranchesShareOneKeyedRuntime() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var planner = (PlannerBase)
                    ((TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode()))
                            .getPlanner();
            var type = RowType.of(new IntType(false));
            var config = new Configuration();
            var distribution = InputProperty.builder()
                    .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                    .build();
            var first = new FutureStateNode(type);
            var second = new FutureStateNode(type);
            for (var node : List.of(first, second)) {
                var exchange = new StreamFusionExecExchange(config, distribution, type, "exchange");
                connect(exchange, new NativeRegionTopologyTest.ArrowSource(type));
                connect(node, exchange);
            }
            var root = new StreamFusionExecUnion(
                    config, List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), type, "union");
            root.setInputEdges(List.of(
                    ExecEdge.builder().source(first).target(root).build(),
                    ExecEdge.builder().source(second).target(root).build()));
            var translated = (KeyedMultipleInputTransformation<?>) root.translateToPlan(planner);
            assertThat(translated.getInputs()).hasSize(2);
            assertThat(translated.getTransitivePredecessors().stream()
                            .filter(node -> node instanceof KeyedMultipleInputTransformation)
                            .count())
                    .isEqualTo(1);
            var identities = translated.getOperatorFactory().getClass().getDeclaredField("stateIds");
            identities.setAccessible(true);
            assertThat(identities.get(translated.getOperatorFactory()))
                    .isEqualTo(List.of((1L << 32) | first.getId(), (1L << 32) | second.getId()));
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    private static void connect(ExecNode<?> target, ExecNode<?> source) {
        target.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(target).build()));
    }

    private static final class FutureStateNode extends ExecNodeBase<RowData>
            implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
        private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

        @Override
        public StreamFusionNativeNodeMetadata nativeMetadata() {
            return nativeMetadata;
        }

        FutureStateNode(RowType type) {
            super(
                    ExecNodeContext.newNodeId(),
                    new ExecNodeContext("future-state_1"),
                    new Configuration(),
                    List.of(InputProperty.DEFAULT),
                    type,
                    "future state");
        }

        @Override
        public boolean ownsNativeKeyedState() {
            return true;
        }

        @Override
        public byte[] nativePlanFragment(PlannerBase planner) {
            return StreamFusionDeduplicateTranslator.createStagePlan(
                    (RowType) getOutputType(),
                    (RowType) getOutputType(),
                    new int[] {0},
                    false,
                    false,
                    true,
                    false,
                    0,
                    new Configuration());
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
            throw new AssertionError("Internal state owners must not become Java operators");
        }
    }
}
