/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;

/** Distinct native lookup node. Only its outer region creates a Flink runtime operator. */
public final class StreamFusionExecLookupJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata metadata = new StreamFusionNativeNodeMetadata();
    private final StreamFusionLookupJoinSupport.Shape lookup;

    StreamFusionExecLookupJoin(
            ReadableConfig config, InputProperty input, RowType output, StreamFusionLookupJoinSupport.Shape lookup) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-lookup-join_1"),
                config,
                List.of(input),
                output,
                "StreamFusionLookupJoin");
        this.lookup = lookup;
    }

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return metadata;
    }

    @Override
    public CsvLookupSnapshotSource lookupSource() {
        return lookup.source;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                "tech.streamfusion.flink.join.StreamFusionLookupJoinPlan",
                new Class<?>[] {RowType.class, RowType.class, RowType.class, int[].class, int[].class},
                getInputEdges().get(0).getOutputType(),
                lookup.sideType,
                getOutputType(),
                lookup.probeKeys,
                lookup.sideKeys);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
