/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
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

/** Distinct StreamFusion physical node for SQL window aggregation. */
public final class StreamFusionExecWindowAggregate extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] aggregateCalls;
    private final WindowingStrategy windowing;
    private final NamedWindowProperty[] namedWindowProperties;
    private final boolean needRetraction;

    public StreamFusionExecWindowAggregate(
            ReadableConfig persistedConfig,
            int[] grouping,
            AggregateCall[] aggregateCalls,
            WindowingStrategy windowing,
            NamedWindowProperty[] namedWindowProperties,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-aggregate_1"),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping.clone();
        this.aggregateCalls = aggregateCalls.clone();
        this.windowing = windowing;
        this.namedWindowProperties = namedWindowProperties.clone();
        this.needRetraction = needRetraction;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        RowType inputType = (RowType) inputEdge.getOutputType();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputType));
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
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
                    inputType,
                    (RowType) getOutputType(),
                    grouping,
                    aggregateCalls,
                    windowing,
                    namedWindowProperties,
                    needRetraction,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion WindowAggregate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion WindowAggregate runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowAggregate translation failed", e.getCause());
        }
    }
}
