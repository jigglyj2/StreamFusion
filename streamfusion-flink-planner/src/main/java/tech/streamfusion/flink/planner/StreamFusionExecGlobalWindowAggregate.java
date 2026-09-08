/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.types.logical.RowType;

/** Native keyed global merge/timer stage of Flink's two-phase window aggregate. */
public final class StreamFusionExecGlobalWindowAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

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

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    @Override
    public boolean ownsNativeKeyedState() {
        return true;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        Configuration config = Configuration.fromMap(
                planner.getTableConfig().getConfiguration().toMap());
        config.addAll(Configuration.fromMap(getPersistedConfig().toMap()));
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                "tech.streamfusion.flink.planner.window.StreamFusionGlobalWindowAggregateTranslator",
                new Class<?>[] {
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    int.class,
                    AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class
                },
                originalInputType,
                getInputEdges().get(0).getOutputType(),
                getOutputType(),
                groupingCount,
                calls,
                windowing,
                properties,
                needRetraction,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
