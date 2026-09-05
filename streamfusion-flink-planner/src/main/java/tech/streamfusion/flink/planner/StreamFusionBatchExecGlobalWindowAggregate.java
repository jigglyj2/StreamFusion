/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.IntStream;
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
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Native bounded global window aggregation merging opaque local accumulators. */
public final class StreamFusionBatchExecGlobalWindowAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {
    private static final String TRANSLATOR =
            "tech.streamfusion.flink.window.StreamFusionGroupWindowAggregateTranslator";

    private final RowType originalInputType;
    private final int groupingCount;
    private final AggregateCall[] calls;
    private final LogicalWindow window;
    private final NamedWindowProperty[] properties;

    public StreamFusionBatchExecGlobalWindowAggregate(
            ReadableConfig config,
            RowType originalInputType,
            int groupingCount,
            AggregateCall[] calls,
            LogicalWindow window,
            NamedWindowProperty[] properties,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-global-window-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionBatchGlobalWindowAggregate");
        this.originalInputType = originalInputType;
        this.groupingCount = groupingCount;
        this.calls = calls.clone();
        this.window = window;
        this.properties = properties.clone();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType internalType = (RowType) edge.getOutputType();
        int[] grouping = IntStream.range(0, groupingCount).toArray();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(internalType));
        try {
            Method method = Class.forName(
                            TRANSLATOR, true, planner.getFlinkContext().getClassLoader())
                    .getMethod(
                            "translateBatchGlobal",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            AggregateCall[].class,
                            LogicalWindow.class,
                            NamedWindowProperty[].class,
                            ReadableConfig.class,
                            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                            RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    originalInputType,
                    internalType,
                    (RowType) getOutputType(),
                    groupingCount,
                    calls,
                    window,
                    properties,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected bounded global window aggregate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the bounded global window aggregate", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Bounded global window aggregate failed", failure.getCause());
        }
    }
}
