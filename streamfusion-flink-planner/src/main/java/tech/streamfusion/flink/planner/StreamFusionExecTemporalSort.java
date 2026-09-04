/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for streaming temporal sort. */
public final class StreamFusionExecTemporalSort extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.sort.StreamFusionTemporalSortTranslator";

    private final SortSpec sortSpec;
    private final boolean processingTime;

    public StreamFusionExecTemporalSort(
            ReadableConfig config,
            SortSpec sortSpec,
            boolean processingTime,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-temporal-sort_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.sortSpec = sortSpec;
        this.processingTime = processingTime;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    SortSpec.class,
                    boolean.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    (RowType) edge.getOutputType(),
                    sortSpec,
                    processingTime,
                    getPersistedConfig(),
                    planner.getExecEnv());
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion TemporalSort failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion TemporalSort runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion TemporalSort translation failed", failure.getCause());
        }
    }
}
