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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Native local half of a two-phase mini-batch group aggregate. */
public final class StreamFusionExecLocalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();
    private static final String TRANSLATOR =
            "tech.streamfusion.flink.planner.aggregate.StreamFusionLocalGroupAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] calls;
    private final boolean[] retractable;
    private final boolean inputChangelog;

    public StreamFusionExecLocalGroupAggregate(
            ReadableConfig config,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean inputChangelog,
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
    }

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        Configuration config = Configuration.fromMap(
                planner.getTableConfig().getConfiguration().toMap());
        config.addAll(Configuration.fromMap(getPersistedConfig().toMap()));
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                TRANSLATOR,
                new Class<?>[] {
                    RowType.class,
                    RowType.class,
                    int[].class,
                    AggregateCall[].class,
                    boolean[].class,
                    boolean.class,
                    ReadableConfig.class
                },
                getInputEdges().get(0).getOutputType(),
                getOutputType(),
                grouping,
                calls,
                retractable,
                inputChangelog,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
