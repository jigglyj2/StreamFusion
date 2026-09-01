/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for event-time Window Join. */
public final class StreamFusionExecWindowJoin extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.window.StreamFusionWindowJoinTranslator";

    private final JoinSpec joinSpec;
    private final WindowingStrategy leftWindowing;
    private final WindowingStrategy rightWindowing;

    public StreamFusionExecWindowJoin(
            ReadableConfig config,
            JoinSpec joinSpec,
            WindowingStrategy leftWindowing,
            WindowingStrategy rightWindowing,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-join_1"),
                config,
                List.of(leftInput, rightInput),
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.leftWindowing = leftWindowing;
        this.rightWindowing = rightWindowing;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftEdge = getInputEdges().get(0);
        ExecEdge rightEdge = getInputEdges().get(1);
        Transformation<RowData> left = (Transformation<RowData>) leftEdge.translateToPlan(planner);
        Transformation<RowData> right = (Transformation<RowData>) rightEdge.translateToPlan(planner);
        RowType leftType = (RowType) leftEdge.getOutputType();
        RowType rightType = (RowType) rightEdge.getOutputType();
        JoinUtil.validateJoinSpec(joinSpec, leftType, rightType, true);
        RowDataKeySelector leftSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), joinSpec.getLeftKeys(), InternalTypeInfo.of(leftType));
        RowDataKeySelector rightSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), joinSpec.getRightKeys(), InternalTypeInfo.of(rightType));
        GeneratedJoinCondition condition = JoinUtil.generateConditionFunction(
                config, planner.getFlinkContext().getClassLoader(), joinSpec, leftType, rightType);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    WindowingStrategy.class,
                    WindowingStrategy.class,
                    GeneratedJoinCondition.class,
                    ReadableConfig.class,
                    StreamExecutionEnvironment.class,
                    RowDataKeySelector.class,
                    RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    left,
                    right,
                    leftType,
                    rightType,
                    (RowType) getOutputType(),
                    joinSpec,
                    leftWindowing,
                    rightWindowing,
                    condition,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    leftSelector,
                    rightSelector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion WindowJoin failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion WindowJoin runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion WindowJoin translation failed", failure.getCause());
        }
    }
}
