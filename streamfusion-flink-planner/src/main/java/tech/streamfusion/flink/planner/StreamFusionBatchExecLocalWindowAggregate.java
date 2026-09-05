/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Native bounded local window aggregation emitting opaque partial accumulators. */
public final class StreamFusionBatchExecLocalWindowAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {
    private static final String TRANSLATOR =
            "tech.streamfusion.flink.window.StreamFusionGroupWindowAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] calls;
    private final LogicalWindow window;

    public StreamFusionBatchExecLocalWindowAggregate(
            ReadableConfig config,
            int[] grouping,
            AggregateCall[] calls,
            LogicalWindow window,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-local-window-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionBatchLocalWindowAggregate");
        this.grouping = grouping.clone();
        this.calls = calls.clone();
        this.window = window;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        try {
            Method method = Class.forName(
                            TRANSLATOR, true, planner.getFlinkContext().getClassLoader())
                    .getMethod(
                            "translateBatchLocal",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            AggregateCall[].class,
                            LogicalWindow.class,
                            ReadableConfig.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    (RowType) edge.getOutputType(),
                    (RowType) getOutputType(),
                    grouping,
                    calls,
                    window,
                    getPersistedConfig());
            if (result == null) {
                throw new IllegalStateException("A selected bounded local window aggregate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the bounded local window aggregate", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Bounded local window aggregate failed", failure.getCause());
        }
    }
}
