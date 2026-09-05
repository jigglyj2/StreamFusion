/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
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
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for synchronous regular streaming join. */
public final class StreamFusionExecRegularJoin extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.join.StreamFusionRegularJoinTranslator";

    private final JoinSpec joinSpec;
    private final List<int[]> leftUpsertKeys;
    private final List<int[]> rightUpsertKeys;
    private final long leftStateTtlMillis;
    private final long rightStateTtlMillis;

    public StreamFusionExecRegularJoin(
            ReadableConfig config,
            JoinSpec joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            long leftStateTtlMillis,
            long rightStateTtlMillis,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-regular-join_1"),
                config,
                List.of(leftInput, rightInput),
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.leftUpsertKeys = leftUpsertKeys;
        this.rightUpsertKeys = rightUpsertKeys;
        this.leftStateTtlMillis = leftStateTtlMillis;
        this.rightStateTtlMillis = rightStateTtlMillis;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return translateInternal(planner, null);
    }

    Transformation<RowData> translateWithOutputCalcs(PlannerBase planner, List<StreamFusionExecCalc> calcs) {
        return translateInternal(planner, calcs);
    }

    @SuppressWarnings("unchecked")
    private Transformation<RowData> translateInternal(PlannerBase planner, List<StreamFusionExecCalc> outputCalcs) {
        ExecEdge leftEdge = getInputEdges().get(0);
        ExecEdge rightEdge = getInputEdges().get(1);
        Transformation<RowData> left = (Transformation<RowData>) leftEdge.translateToPlan(planner);
        Transformation<RowData> right = (Transformation<RowData>) rightEdge.translateToPlan(planner);
        RowType leftType = (RowType) leftEdge.getOutputType();
        RowType rightType = (RowType) rightEdge.getOutputType();
        RowDataKeySelector leftSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), joinSpec.getLeftKeys(), InternalTypeInfo.of(leftType));
        RowDataKeySelector rightSelector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), joinSpec.getRightKeys(), InternalTypeInfo.of(rightType));
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method;
            Transformation<RowData> result;
            if (outputCalcs == null) {
                method = translator.getMethod(
                        "translate",
                        Transformation.class,
                        Transformation.class,
                        RowType.class,
                        RowType.class,
                        RowType.class,
                        JoinSpec.class,
                        List.class,
                        List.class,
                        long.class,
                        long.class,
                        ReadableConfig.class,
                        StreamExecutionEnvironment.class,
                        RowDataKeySelector.class,
                        RowDataKeySelector.class);
                result = (Transformation<RowData>) method.invoke(
                        null,
                        left,
                        right,
                        leftType,
                        rightType,
                        (RowType) getOutputType(),
                        joinSpec,
                        leftUpsertKeys,
                        rightUpsertKeys,
                        leftStateTtlMillis,
                        rightStateTtlMillis,
                        getPersistedConfig(),
                        planner.getExecEnv(),
                        leftSelector,
                        rightSelector);
            } else {
                List<RowType> calcInputTypes = new ArrayList<>(outputCalcs.size());
                List<RowType> calcOutputTypes = new ArrayList<>(outputCalcs.size());
                List<List<RexNode>> projections = new ArrayList<>(outputCalcs.size());
                List<RexNode> conditions = new ArrayList<>(outputCalcs.size());
                for (StreamFusionExecCalc calc : outputCalcs) {
                    calcInputTypes.add((RowType) calc.getInputEdges().get(0).getOutputType());
                    calcOutputTypes.add((RowType) calc.getOutputType());
                    projections.add(calc.streamFusionProjection());
                    conditions.add(calc.streamFusionCondition());
                }
                method = translator.getMethod(
                        "translateWithOutputCalcs",
                        Transformation.class,
                        Transformation.class,
                        RowType.class,
                        RowType.class,
                        RowType.class,
                        RowType.class,
                        JoinSpec.class,
                        List.class,
                        List.class,
                        long.class,
                        long.class,
                        ReadableConfig.class,
                        StreamExecutionEnvironment.class,
                        RowDataKeySelector.class,
                        RowDataKeySelector.class,
                        List.class,
                        List.class,
                        List.class,
                        List.class);
                result = (Transformation<RowData>) method.invoke(
                        null,
                        left,
                        right,
                        leftType,
                        rightType,
                        (RowType) getOutputType(),
                        calcOutputTypes.get(calcOutputTypes.size() - 1),
                        joinSpec,
                        leftUpsertKeys,
                        rightUpsertKeys,
                        leftStateTtlMillis,
                        rightStateTtlMillis,
                        getPersistedConfig(),
                        planner.getExecEnv(),
                        leftSelector,
                        rightSelector,
                        calcInputTypes,
                        calcOutputTypes,
                        projections,
                        conditions);
            }
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion regular join failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion regular join runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion regular join translation failed", failure.getCause());
        }
    }
}
