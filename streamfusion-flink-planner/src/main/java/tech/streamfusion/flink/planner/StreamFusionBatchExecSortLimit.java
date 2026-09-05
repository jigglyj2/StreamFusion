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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for one local or global bounded SortLimit stage. */
public final class StreamFusionBatchExecSortLimit extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.sort.StreamFusionBoundedSortLimitTranslator";

    private final SortSpec sortSpec;
    private final long limitStart;
    private final long limitEnd;
    private final boolean global;

    public StreamFusionBatchExecSortLimit(
            ReadableConfig config,
            SortSpec sortSpec,
            long limitStart,
            long limitEnd,
            boolean global,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-sort-limit_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.sortSpec = sortSpec;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
        this.global = global;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        try {
            Class<?> translator =
                    Class.forName(TRANSLATOR, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    SortSpec.class,
                    long.class,
                    long.class,
                    boolean.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    (RowType) edge.getOutputType(),
                    sortSpec,
                    limitStart,
                    limitEnd,
                    global,
                    getPersistedConfig(),
                    planner.getExecEnv());
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion bounded SortLimit failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion bounded SortLimit runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded SortLimit translation failed", failure.getCause());
        }
    }
}
