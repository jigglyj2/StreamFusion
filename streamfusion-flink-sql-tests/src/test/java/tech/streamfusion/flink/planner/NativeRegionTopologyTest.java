/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Direct selected-graph translation: production admission remains separately tested. */
class NativeRegionTopologyTest {
    @Test
    void runtimePreflightAcceptsTheCompletePackagedPlannerRuntime() {
        assertThat(StreamFusionExecGraphProcessor.runtimePreflightRejection(
                        getClass().getClassLoader()))
                .isNull();
    }

    @Test
    void multiplePersistentOwnersAndMixedFragmentsUseTheCommonKeyedRegion() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            TableEnvironmentImpl environment =
                    (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            PlannerBase planner = (PlannerBase) environment.getPlanner();
            Configuration config = new Configuration();
            RowType inputType = RowType.of(new IntType(false));
            RowType joinType = RowType.of(new IntType(false), new IntType(false));
            var left = new ArrowSource(inputType);
            var right = new ArrowSource(inputType);
            var distribution = InputProperty.builder()
                    .requiredDistribution(InputProperty.hashDistribution(new int[] {0}))
                    .build();
            var leftExchange =
                    unary(new StreamFusionExecExchange(config, distribution, inputType, "left exchange"), left);
            var rightExchange =
                    unary(new StreamFusionExecExchange(config, distribution, inputType, "right exchange"), right);
            var join = new StreamFusionExecRegularJoin(
                    config,
                    new org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec(
                            FlinkJoinType.INNER, new int[] {0}, new int[] {0}, new boolean[] {true}, null),
                    List.of(),
                    List.of(),
                    0,
                    0,
                    InputProperty.DEFAULT,
                    InputProperty.DEFAULT,
                    joinType,
                    "join");
            join.setInputEdges(List.of(
                    ExecEdge.builder().source(leftExchange).target(join).build(),
                    ExecEdge.builder().source(rightExchange).target(join).build()));
            FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
            RexBuilder rex = new RexBuilder(types);
            RexNode key = rex.makeInputRef(types.createFieldTypeFromLogicalType(new IntType(false)), 0);
            RexNode value = rex.makeInputRef(types.createFieldTypeFromLogicalType(new IntType(false)), 1);
            var expand = unary(
                    new StreamFusionExecExpand(
                            config,
                            List.of(List.of(key, value), List.of(key, value)),
                            InputProperty.DEFAULT,
                            joinType,
                            "expand"),
                    join);
            var calc = unary(
                    new StreamFusionExecCalc(config, List.of(key), null, InputProperty.DEFAULT, inputType, "calc"),
                    expand);
            var dedup = unary(
                    new StreamFusionExecDeduplicate(
                            config,
                            new int[] {0},
                            false,
                            false,
                            true,
                            false,
                            null,
                            InputProperty.DEFAULT,
                            inputType,
                            "dedup"),
                    calc);
            var originalDedup = new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate(
                    config,
                    new int[] {0},
                    false,
                    false,
                    true,
                    false,
                    InputProperty.DEFAULT,
                    inputType,
                    "original dedup");
            originalDedup.setCompiled(true);
            new StreamFusionGraphRewrite().convert(originalDedup, ignored -> dedup);
            Transformation<?> result = dedup.translateToPlan(planner);
            assertThat(result)
                    .isInstanceOf(
                            org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation.class);
            var owner = (org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation<?>) result;
            assertThat(owner.getOperatorFactory())
                    .isInstanceOf(tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory.class);
            assertThat(owner.getStateKeyType()).isEqualTo(org.apache.flink.api.common.typeinfo.Types.INT);
            assertThat(owner.getStateKeySelectors()).hasSize(2);
            assertThat(owner.getManagedMemoryOperatorScopeUseCaseWeights())
                    .containsEntry(org.apache.flink.core.memory.ManagedMemoryUseCase.OPERATOR, 16);
            assertThat(result.getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
            assertThat(result.getTransitivePredecessors())
                    .noneMatch(node -> node instanceof OneInputTransformation
                            && ((OneInputTransformation<?, ?>) node).getOperator()
                                    instanceof StreamFusionArrowNativeOperator);
            assertThat(result.getTransitivePredecessors().stream()
                            .filter(node -> node instanceof OneInputTransformation)
                            .count())
                    .isEqualTo(2); // only external IPC writers, no decode/re-encode or per-state runtime.
            var ids = owner.getOperatorFactory().getClass().getDeclaredField("stateIds");
            ids.setAccessible(true);
            assertThat((List<?>) ids.get(owner.getOperatorFactory()))
                    .isEqualTo(List.of((1L << 32) | join.getId(), (1L << 32) | originalDedup.getId()));
            var field = owner.getOperatorFactory().getClass().getDeclaredField("plan");
            field.setAccessible(true);
            NativePlan plan = NativePlan.parseFrom((byte[]) field.get(owner.getOperatorFactory()));
            assertThat(plan.getProtocolVersion()).isEqualTo(3);
            assertThat(plan.getRoot().getPlanNodeId()).isEqualTo((1L << 32) | originalDedup.getId());
            assertThat(plan.getRoot().getMetricName())
                    .isEqualTo(dedup.nativeMetadata().metricName(dedup, planner.getTableConfig()));
            assertThat(plan.getRoot().hasMetricUid()).isTrue();
            assertThat(plan.getRoot().getMetricUid()).isEqualTo(originalDedup.getId() + "_deduplicate");
            assertThat(originalDedup.getTransformation()).isNull();
            var calcPlan = plan.getRoot().getDeduplicate().getInput();
            assertThat(calcPlan.getCalc().getPreserveInputEnvelope()).isTrue();
            assertThat(calcPlan.getPlanNodeId()).isEqualTo((1L << 32) | calc.getId());
            assertThat(calcPlan.getCalc().getInput().getPlanNodeId()).isEqualTo((1L << 32) | expand.getId());
            assertThat(calcPlan.getCalc().getInput().getExpand().getInput().hasRegularJoin())
                    .isTrue();
            assertThat(left.translations).isOne();
            assertThat(right.translations).isOne();
        } finally {
            if (previous == null) System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            else System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
        }
    }

    @Test
    void plannerBuildsOneArrowRuntimeForMixedRegionsInBothExecutionModes() throws Exception {
        String previous = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            for (boolean bounded : List.of(false, true)) {
                TableEnvironmentImpl environment = (TableEnvironmentImpl) TableEnvironment.create(
                        bounded ? EnvironmentSettings.inBatchMode() : EnvironmentSettings.inStreamingMode());
                PlannerBase planner = (PlannerBase) environment.getPlanner();
                Configuration config = new Configuration();
                RowType inputType = RowType.of(new IntType(false), new ArrayType(new IntType()));
                RowType expandedType = RowType.of(inputType.getTypeAt(0), inputType.getTypeAt(1), new IntType());
                RowType outputType = RowType.of(new IntType(false), new IntType());
                FlinkTypeFactory types = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
                RexBuilder rex = new RexBuilder(types);
                RexNode key = rex.makeInputRef(types.createFieldTypeFromLogicalType(inputType.getTypeAt(0)), 0);
                RexNode array = rex.makeInputRef(types.createFieldTypeFromLogicalType(inputType.getTypeAt(1)), 1);
                RexNode item = rex.makeInputRef(types.createFieldTypeFromLogicalType(new IntType()), 2);
                RexCall invocation = (RexCall) rex.makeCall(
                        types.createFieldTypeFromLogicalType(new IntType()),
                        new SqlFunction(
                                "$UNNEST_ROWS$1",
                                SqlKind.OTHER_FUNCTION,
                                null,
                                null,
                                null,
                                SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION),
                        List.of(array));
                ArrowSource source = new ArrowSource(inputType);
                ExecNode<?> first = unary(
                        bounded
                                ? new StreamFusionBatchExecCalc(
                                        config, List.of(key, array), null, InputProperty.DEFAULT, inputType, "first")
                                : new StreamFusionExecCalc(
                                        config, List.of(key, array), null, InputProperty.DEFAULT, inputType, "first"),
                        source);
                ExecNode<?> unnest = unary(
                        bounded
                                ? new StreamFusionBatchExecArrayUnnest(
                                        config,
                                        FlinkJoinType.INNER,
                                        invocation,
                                        InputProperty.DEFAULT,
                                        expandedType,
                                        "unnest")
                                : new StreamFusionExecArrayUnnest(
                                        config,
                                        FlinkJoinType.INNER,
                                        invocation,
                                        InputProperty.DEFAULT,
                                        expandedType,
                                        "unnest"),
                        first);
                ExecNode<?> expand = unary(
                        bounded
                                ? new StreamFusionBatchExecExpand(
                                        config,
                                        List.of(List.of(key, array, item)),
                                        InputProperty.DEFAULT,
                                        expandedType,
                                        "expand")
                                : new StreamFusionExecExpand(
                                        config,
                                        List.of(List.of(key, array, item)),
                                        InputProperty.DEFAULT,
                                        expandedType,
                                        "expand"),
                        unnest);
                ExecNode<?> root = unary(
                        bounded
                                ? new StreamFusionBatchExecCalc(
                                        config, List.of(key, item), null, InputProperty.DEFAULT, outputType, "last")
                                : new StreamFusionExecCalc(
                                        config, List.of(key, item), null, InputProperty.DEFAULT, outputType, "last"),
                        expand);
                var original = bounded
                        ? new org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc(
                                config, List.of(key, item), null, InputProperty.DEFAULT, outputType, "original last")
                        : new org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc(
                                config, List.of(key, item), null, InputProperty.DEFAULT, outputType, "original last");
                original.setCompiled(true);
                new StreamFusionGraphRewrite().convert(original, ignored -> root);
                Transformation<?> result = root.translateToPlan(planner);
                var operators = result.getTransitivePredecessors().stream()
                        .filter(node -> node instanceof OneInputTransformation)
                        .map(node -> (OneInputTransformation<?, ?>) node)
                        .collect(java.util.stream.Collectors.toList());
                assertThat(operators).hasSize(1);
                assertThat(operators.get(0).getOperator()).isInstanceOf(StreamFusionArrowNativeOperator.class);
                assertThat(operators.get(0).getInputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
                assertThat(operators.get(0).getOutputType()).isSameAs(ArrowRowDataBatchTypeInfo.INSTANCE);
                assertThat(source.translations).isOne();
                var field = StreamFusionArrowNativeOperator.class.getDeclaredField("serializedPlan");
                field.setAccessible(true);
                NativePlan nativePlan =
                        NativePlan.parseFrom((byte[]) field.get(operators.get(0).getOperator()));
                var node = nativePlan.getRoot();
                assertThat(node.hasCalc()).isTrue();
                assertThat(node.getPlanNodeId()).isEqualTo((1L << 32) | original.getId());
                assertThat(node.hasMetricUid()).isTrue();
                assertThat(node.getMetricUid()).isEqualTo(original.getId() + "_calc");
                assertThat(node.getMetricName())
                        .isEqualTo(((StreamFusionNativePlanNode) root)
                                .nativeMetadata()
                                .metricName(root, planner.getTableConfig()));
                assertThat(original.getTransformation()).isNull();
                node = node.getCalc().getInput();
                assertThat(node.hasExpand()).isTrue();
                assertThat(node.getPlanNodeId()).isEqualTo((1L << 32) | expand.getId());
                node = node.getExpand().getInput();
                assertThat(node.hasArrayUnnest()).isTrue();
                assertThat(node.getPlanNodeId()).isEqualTo((1L << 32) | unnest.getId());
                node = node.getArrayUnnest().getInput();
                assertThat(node.hasCalc()).isTrue();
                assertThat(node.getPlanNodeId()).isEqualTo((1L << 32) | first.getId());
                node = node.getCalc().getInput();
                assertThat(node.hasInput()).isTrue();
            }
        } finally {
            if (previous == null) {
                System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            } else {
                System.setProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, previous);
            }
        }
    }

    private static <T extends ExecNode<?>> T unary(T node, ExecNode<?> input) {
        node.setInputEdges(List.of(ExecEdge.builder().source(input).target(node).build()));
        return node;
    }

    static final class ArrowSource extends ExecNodeBase<RowData> {
        int translations;

        ArrowSource(RowType type) {
            super(
                    ExecNodeContext.newNodeId(),
                    new ExecNodeContext("test-arrow-source_1"),
                    new Configuration(),
                    List.of(),
                    type,
                    "source");
            setInputEdges(List.of());
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
            translations++;
            Transformation<ArrowRowDataBatch> input =
                    new Transformation<ArrowRowDataBatch>("Arrow source", ArrowRowDataBatchTypeInfo.INSTANCE, 1) {
                        @Override
                        protected List<Transformation<?>> getTransitivePredecessorsInternal() {
                            return List.of(this);
                        }

                        @Override
                        public List<Transformation<?>> getInputs() {
                            return List.of();
                        }
                    };
            return StreamFusionArrowBoundaries.asPlannerTransformation(input);
        }
    }
}
