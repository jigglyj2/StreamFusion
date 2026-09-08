/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Native buffered local slice stage of Flink's two-phase window aggregate. */
public final class StreamFusionExecLocalWindowAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

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

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    @Override
    public boolean ownsLocalWindowBuffer() {
        return true;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        var config = org.apache.flink.configuration.Configuration.fromMap(
                planner.getTableConfig().toMap());
        config.addAll(org.apache.flink.configuration.Configuration.fromMap(
                getPersistedConfig().toMap()));
        return tech.streamfusion.flink.planner.window.StreamFusionLocalWindowAggregateTranslator.createStagePlan(
                (RowType) getInputEdges().get(0).getOutputType(),
                (RowType) getOutputType(),
                grouping,
                calls,
                windowing,
                needRetraction,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
