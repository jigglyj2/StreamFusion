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

/** Native middle stage of a local/incremental/global mini-batch aggregate chain. */
public final class StreamFusionExecIncrementalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final String TRANSLATOR =
            "tech.streamfusion.flink.aggregate.StreamFusionIncrementalGroupAggregateTranslator";

    private final RowType partialOriginalInputType;
    private final int partialGroupingCount;
    private final int[] finalGrouping;
    private final AggregateCall[] partialCalls;
    private final boolean[] partialRetractable;
    private final RowType finalOriginalInputType;
    private final AggregateCall[] finalCalls;
    private final boolean[] finalRetractable;
    private final long miniBatchSize;

    public StreamFusionExecIncrementalGroupAggregate(
            ReadableConfig config,
            RowType partialOriginalInputType,
            int partialGroupingCount,
            int[] finalGrouping,
            AggregateCall[] partialCalls,
            boolean[] partialRetractable,
            RowType finalOriginalInputType,
            AggregateCall[] finalCalls,
            boolean[] finalRetractable,
            long miniBatchSize,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-incremental-group-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionIncrementalGroupAggregate");
        this.partialOriginalInputType = partialOriginalInputType;
        this.partialGroupingCount = partialGroupingCount;
        this.finalGrouping = finalGrouping.clone();
        this.partialCalls = partialCalls.clone();
        this.partialRetractable = partialRetractable.clone();
        this.finalOriginalInputType = finalOriginalInputType;
        this.finalCalls = finalCalls.clone();
        this.finalRetractable = finalRetractable.clone();
        this.miniBatchSize = miniBatchSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType internalInputType = (RowType) edge.getOutputType();
        int[] grouping =
                java.util.stream.IntStream.range(0, partialGroupingCount).toArray();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(internalInputType));
        try {
            Method method = Class.forName(
                            TRANSLATOR, true, planner.getFlinkContext().getClassLoader())
                    .getMethod(
                            "translate",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            int[].class,
                            AggregateCall[].class,
                            boolean[].class,
                            RowType.class,
                            AggregateCall[].class,
                            boolean[].class,
                            long.class,
                            RowDataKeySelector.class);
            return (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    partialOriginalInputType,
                    internalInputType,
                    (RowType) getOutputType(),
                    partialGroupingCount,
                    finalGrouping,
                    partialCalls,
                    partialRetractable,
                    finalOriginalInputType,
                    finalCalls,
                    finalRetractable,
                    miniBatchSize,
                    selector);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the native incremental group aggregate", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Native incremental group aggregate translation failed", e.getCause());
        }
    }
}
