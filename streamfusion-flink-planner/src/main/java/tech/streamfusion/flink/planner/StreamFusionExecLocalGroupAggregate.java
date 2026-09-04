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

/** Native local half of a two-phase mini-batch group aggregate. */
public final class StreamFusionExecLocalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final String TRANSLATOR =
            "tech.streamfusion.flink.aggregate.StreamFusionLocalGroupAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] calls;
    private final boolean[] retractable;
    private final boolean inputChangelog;
    private final long miniBatchSize;

    public StreamFusionExecLocalGroupAggregate(
            ReadableConfig config,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean inputChangelog,
            long miniBatchSize,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-local-group-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionLocalGroupAggregate");
        this.grouping = grouping.clone();
        this.calls = calls.clone();
        this.retractable = retractable.clone();
        this.inputChangelog = inputChangelog;
        this.miniBatchSize = miniBatchSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType inputType = (RowType) edge.getOutputType();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputType));
        try {
            Method method = Class.forName(
                            TRANSLATOR, true, planner.getFlinkContext().getClassLoader())
                    .getMethod(
                            "translate",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            AggregateCall[].class,
                            boolean[].class,
                            boolean.class,
                            long.class,
                            RowDataKeySelector.class);
            return (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    grouping,
                    calls,
                    retractable,
                    inputChangelog,
                    miniBatchSize,
                    selector);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the native local group aggregate", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Native local group aggregate translation failed", e.getCause());
        }
    }
}
