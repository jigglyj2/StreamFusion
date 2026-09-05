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
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Native state-free local slice stage of Flink's two-phase window aggregate. */
public final class StreamFusionExecLocalWindowAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.window.StreamFusionWindowAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] calls;
    private final WindowingStrategy windowing;
    private final boolean needRetraction;

    public StreamFusionExecLocalWindowAggregate(
            ReadableConfig config,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy windowing,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-local-window-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionLocalWindowAggregate");
        this.grouping = grouping.clone();
        this.calls = calls.clone();
        this.windowing = windowing;
        this.needRetraction = needRetraction;
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
                            "translateLocal",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            AggregateCall[].class,
                            WindowingStrategy.class,
                            boolean.class,
                            ReadableConfig.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    (RowType) edge.getOutputType(),
                    (RowType) getOutputType(),
                    grouping,
                    calls,
                    windowing,
                    needRetraction,
                    getPersistedConfig());
            if (result == null) {
                throw new IllegalStateException("A selected native local window aggregate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the native local window aggregate", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Native local window aggregate translation failed", e.getCause());
        }
    }
}
