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
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for bounded event/proctime streaming joins. */
public final class StreamFusionExecIntervalJoin extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.join.StreamFusionIntervalJoinTranslator";

    private final IntervalJoinSpec intervalJoinSpec;

    public StreamFusionExecIntervalJoin(
            ReadableConfig config,
            IntervalJoinSpec intervalJoinSpec,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-interval-join_1"),
                config,
                List.of(leftInput, rightInput),
                outputType,
                description);
        this.intervalJoinSpec = intervalJoinSpec;
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
        int[] leftKeys = intervalJoinSpec.getJoinSpec().getLeftKeys();
        int[] rightKeys = intervalJoinSpec.getJoinSpec().getRightKeys();
        RowDataKeySelector leftSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), leftKeys, InternalTypeInfo.of(leftType));
        RowDataKeySelector rightSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), rightKeys, InternalTypeInfo.of(rightType));
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
                    IntervalJoinSpec.class,
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
                    intervalJoinSpec,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    leftSelector,
                    rightSelector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion interval join failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion interval join runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion interval join translation failed", failure.getCause());
        }
    }
}
