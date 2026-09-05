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
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Native keyed global merge/timer stage of Flink's two-phase window aggregate. */
public final class StreamFusionExecGlobalWindowAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.window.StreamFusionWindowAggregateTranslator";

    private final RowType originalInputType;
    private final int groupingCount;
    private final AggregateCall[] calls;
    private final WindowingStrategy windowing;
    private final NamedWindowProperty[] properties;
    private final boolean needRetraction;

    public StreamFusionExecGlobalWindowAggregate(
            ReadableConfig config,
            RowType originalInputType,
            int groupingCount,
            AggregateCall[] calls,
            WindowingStrategy windowing,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-global-window-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionGlobalWindowAggregate");
        this.originalInputType = originalInputType;
        this.groupingCount = groupingCount;
        this.calls = calls.clone();
        this.windowing = windowing;
        this.properties = properties.clone();
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType inputType = (RowType) edge.getOutputType();
        int[] grouping = java.util.stream.IntStream.range(0, groupingCount).toArray();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputType));
        try {
            Method method = Class.forName(
                            TRANSLATOR, true, planner.getFlinkContext().getClassLoader())
                    .getMethod(
                            "translateGlobal",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            AggregateCall[].class,
                            WindowingStrategy.class,
                            NamedWindowProperty[].class,
                            boolean.class,
                            ReadableConfig.class,
                            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                            RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    originalInputType,
                    inputType,
                    (RowType) getOutputType(),
                    groupingCount,
                    calls,
                    windowing,
                    properties,
                    needRetraction,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected native global window aggregate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the native global window aggregate", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Native global window aggregate translation failed", e.getCause());
        }
    }
}
