/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for fixed-sequence MATCH_RECOGNIZE. */
public final class StreamFusionExecMatchRecognize extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.match.StreamFusionMatchRecognizeTranslator";

    private final int[] partitionKeys;
    private final List<String> variableNames;
    private final List<RexNode> conditions;
    private final int[] measureVariables;
    private final int[] measureFields;
    private final boolean skipPastLastRow;

    public StreamFusionExecMatchRecognize(
            ReadableConfig persistedConfig,
            int[] partitionKeys,
            List<String> variableNames,
            List<RexNode> conditions,
            int[] measureVariables,
            int[] measureFields,
            boolean skipPastLastRow,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-match-recognize_1"),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionKeys = partitionKeys.clone();
        this.variableNames = List.copyOf(variableNames);
        this.conditions = Collections.unmodifiableList(new java.util.ArrayList<>(conditions));
        this.measureVariables = measureVariables.clone();
        this.measureFields = measureFields.clone();
        this.skipPastLastRow = skipPastLastRow;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        RowType inputType = (RowType) inputEdge.getOutputType();
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), partitionKeys, InternalTypeInfo.of(inputType));
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
                    List.class,
                    List.class,
                    int[].class,
                    int[].class,
                    boolean.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    partitionKeys,
                    variableNames,
                    conditions,
                    measureVariables,
                    measureFields,
                    skipPastLastRow,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion MATCH_RECOGNIZE failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion MATCH_RECOGNIZE runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion MATCH_RECOGNIZE translation failed", e.getCause());
        }
    }
}
