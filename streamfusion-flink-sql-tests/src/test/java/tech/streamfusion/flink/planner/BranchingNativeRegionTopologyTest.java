/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
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
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeWriterOperator;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

/** A future physical family needs only its fragment, not additions to discovery or fusion rules. */
class BranchingNativeRegionTopologyTest {
    @Test
    void unknownBranchingPhysicalContractLowersToOneTreeWithOnlyExternalTransportEdges() throws Exception {
        assertRegion(false, true);
    }

    @Test
    void realStreamingAndBoundedUnionNodesUseTheSharedTree() throws Exception {
        assertRegion(false, false);
        assertRegion(true, false);
    }

    private void assertRegion(boolean bounded, boolean future) throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var environment = (TableEnvironmentImpl) TableEnvironment.create(
                    bounded ? EnvironmentSettings.inBatchMode() : EnvironmentSettings.inStreamingMode());
            var planner = (PlannerBase) environment.getPlanner();
            var type = RowType.of(new IntType(false));
            var config = new Configuration();
            var types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
            var field = new RexBuilder(types).makeInputRef(types.createFieldTypeFromLogicalType(type.getTypeAt(0)), 0);
            var first = new NativeRegionTopologyTest.ArrowSource(type);
            var second = new NativeRegionTopologyTest.ArrowSource(type);
            var left = unary(
                    bounded
                            ? new StreamFusionBatchExecCalc(
                                    config, List.of(field), null, InputProperty.DEFAULT, type, "left")
                            : new StreamFusionExecCalc(
                                    config, List.of(field), null, InputProperty.DEFAULT, type, "left"),
                    first);
            var right = unary(
                    bounded
                            ? new StreamFusionBatchExecCalc(
                                    config, List.of(field), null, InputProperty.DEFAULT, type, "right")
                            : new StreamFusionExecCalc(
                                    config, List.of(field), null, InputProperty.DEFAULT, type, "right"),
                    second);
            ExecNode<?> branch = future
                    ? new FutureNativeBranch(type)
                    : bounded
                            ? new StreamFusionBatchExecUnion(
                                    config, List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), type, "union")
                            : new StreamFusionExecUnion(
                                    config, List.of(InputProperty.DEFAULT, InputProperty.DEFAULT), type, "union");
            branch.setInputEdges(List.of(
                    ExecEdge.builder().source(left).target(branch).build(),
                    ExecEdge.builder().source(right).target(branch).build()));
            var root = unary(
                    bounded
                            ? new StreamFusionBatchExecCalc(
                                    config, List.of(field), null, InputProperty.DEFAULT, type, "root")
                            : new StreamFusionExecCalc(
                                    config, List.of(field), null, InputProperty.DEFAULT, type, "root"),
                    branch);
            Transformation<?> result = root.translateToPlan(planner);
            assertThat(result).isInstanceOf(MultipleInputTransformation.class);
            var region = (MultipleInputTransformation<?>) result;
            assertThat(region.getOperatorFactory()).isInstanceOf(StreamFusionNativeRegionOperatorFactory.class);
            assertThat(region.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(region.getInputs()).hasSize(2);
            for (Transformation<?> edge : region.getInputs()) {
                assertThat(edge).isInstanceOf(OneInputTransformation.class);
                var writer = (OneInputTransformation<?, ?>) edge;
                assertThat(writer.getOperator()).isInstanceOf(NativeExchangeWriterOperator.class);
                assertThat(writer.getInputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            }
            assertThat(result.getTransitivePredecessors().stream()
                            .filter(node -> node instanceof MultipleInputTransformation)
                            .count())
                    .isEqualTo(1);
            assertThat(result.getTransitivePredecessors().stream()
                            .filter(node -> node instanceof OneInputTransformation)
                            .count())
                    .isEqualTo(2);
            assertThat(first.translations).isOne();
            assertThat(second.translations).isOne();
            var bytes = StreamFusionNativeRegionOperatorFactory.class.getDeclaredField("plan");
            bytes.setAccessible(true);
            var plan = NativePlan.parseFrom((byte[]) bytes.get(region.getOperatorFactory()));
            assertThat(plan.getRoot().getPlanNodeId()).isEqualTo((1L << 32) | root.getId());
            var union = plan.getRoot().getCalc().getInput();
            assertThat(union.getPlanNodeId()).isEqualTo((1L << 32) | branch.getId());
            assertThat(union.getUnion().getInputs(0).getPlanNodeId()).isEqualTo((1L << 32) | left.getId());
            assertThat(union.getUnion().getInputs(1).getPlanNodeId()).isEqualTo((1L << 32) | right.getId());
            for (int index = 0; index < 2; index++) {
                assertThat(union.getUnion()
                                .getInputs(index)
                                .getCalc()
                                .getInput()
                                .getInput()
                                .getInputIndex())
                        .isEqualTo(index);
            }
        } finally {
            if (previous == null) {
                System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            } else {
                System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
            }
        }
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> source) {
        node.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(node).build()));
        return node;
    }

    private static final class FutureNativeBranch extends ExecNodeBase<RowData>
            implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
        private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

        @Override
        public StreamFusionNativeNodeMetadata nativeMetadata() {
            return nativeMetadata;
        }

        private FutureNativeBranch(RowType type) {
            super(
                    ExecNodeContext.newNodeId(),
                    new ExecNodeContext("future-branch_1"),
                    new Configuration(),
                    List.of(InputProperty.DEFAULT, InputProperty.DEFAULT),
                    type,
                    "future branch");
        }

        @Override
        public byte[] nativePlanFragment(PlannerBase planner) {
            return NativePlan.newBuilder()
                    .setProtocolVersion(2)
                    .setRoot(Operator.newBuilder()
                            .setUnion(Union.newBuilder()
                                    .addInputs(Operator.newBuilder()
                                            .setInput(Input.newBuilder().setInputIndex(0)))
                                    .addInputs(Operator.newBuilder()
                                            .setInput(Input.newBuilder().setInputIndex(1)))))
                    .build()
                    .toByteArray();
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
            throw new AssertionError("Internal native stages must never be translated to Java operators");
        }
    }
}
