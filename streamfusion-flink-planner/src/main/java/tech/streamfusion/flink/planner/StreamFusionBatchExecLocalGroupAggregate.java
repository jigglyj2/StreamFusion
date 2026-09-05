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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Native bounded local aggregation that emits one opaque partial per key and Arrow batch. */
public final class StreamFusionBatchExecLocalGroupAggregate extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {
    private static final String TRANSLATOR =
            "tech.streamfusion.flink.aggregate.StreamFusionLocalGroupAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] calls;
    private final boolean hashAggregateMetrics;

    public StreamFusionBatchExecLocalGroupAggregate(
            ReadableConfig config,
            int[] grouping,
            AggregateCall[] calls,
            InputProperty inputProperty,
            RowType outputType,
            boolean hashAggregateMetrics) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-local-group-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionBatchLocalGroupAggregate");
        this.grouping = grouping.clone();
        this.calls = calls.clone();
        this.hashAggregateMetrics = hashAggregateMetrics;
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
                            "translateBatch",
                            Transformation.class,
                            RowType.class,
                            RowType.class,
                            int[].class,
                            AggregateCall[].class,
                            RowDataKeySelector.class,
                            boolean.class);
            return (Transformation<RowData>) method.invoke(
                    null, input, inputType, (RowType) getOutputType(), grouping, calls, selector, hashAggregateMetrics);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the bounded native local aggregate", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Bounded native local aggregate failed", failure.getCause());
        }
    }
}
