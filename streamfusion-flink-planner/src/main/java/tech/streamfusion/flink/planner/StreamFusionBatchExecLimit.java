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
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for one bounded LIMIT stage. */
public final class StreamFusionBatchExecLimit extends ExecNodeBase<RowData> implements BatchExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.limit.StreamFusionBoundedLimitTranslator";

    private final long start;
    private final long end;
    private final boolean global;

    public StreamFusionBatchExecLimit(
            ReadableConfig config,
            long start,
            long end,
            boolean global,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-limit_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.start = start;
        this.end = end;
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
                    "translate", Transformation.class, RowType.class, long.class, long.class, boolean.class);
            Transformation<RowData> result = (Transformation<RowData>)
                    method.invoke(null, input, (RowType) edge.getOutputType(), start, end, global);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion bounded LIMIT failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion bounded LIMIT runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion bounded LIMIT translation failed", failure.getCause());
        }
    }
}
