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

/** Native persistent global half of a two-phase mini-batch group aggregate. */
public final class StreamFusionExecGlobalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator";

    private final RowType originalInputType;
    private final int groupingCount;
    private final AggregateCall[] calls;
    private final boolean[] retractable;
    private final boolean generateUpdateBefore;

    public StreamFusionExecGlobalGroupAggregate(
            ReadableConfig config,
            RowType originalInputType,
            int groupingCount,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            InputProperty inputProperty,
            RowType outputType) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-global-group-aggregate_1"),
                config,
                List.of(inputProperty),
                outputType,
                "StreamFusionGlobalGroupAggregate");
        this.originalInputType = originalInputType;
        this.groupingCount = groupingCount;
        this.calls = calls.clone();
        this.retractable = retractable.clone();
        this.generateUpdateBefore = generateUpdateBefore;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType internalType = (RowType) edge.getOutputType();
        int[] grouping = java.util.stream.IntStream.range(0, groupingCount).toArray();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(internalType));
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
                            boolean[].class,
                            boolean.class,
                            ReadableConfig.class,
                            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                            RowDataKeySelector.class);
            return (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    originalInputType,
                    internalType,
                    (RowType) getOutputType(),
                    groupingCount,
                    calls,
                    retractable,
                    generateUpdateBefore,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the native global group aggregate", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Native global group aggregate translation failed", e.getCause());
        }
    }
}
