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
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Global partial-accumulator fragment; the common region owns routing, state and runtime. */
public final class StreamFusionExecGlobalGroupAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();
    private final RowType originalInputType;
    private final int groupingCount;
    private final AggregateCall[] calls;
    private final boolean[] retractable;
    private final boolean generateUpdateBefore;
    private final boolean needRetraction;
    private final List<StateMetadata> stateMetadata;

    public StreamFusionExecGlobalGroupAggregate(
            ReadableConfig config,
            RowType originalInputType,
            int groupingCount,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean needRetraction,
            List<StateMetadata> stateMetadata,
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
        this.needRetraction = needRetraction;
        this.stateMetadata = stateMetadata;
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
        long retention =
                StateMetadata.getStateTtlForOneInputOperator(ExecNodeConfig.ofNodeConfig(config, false), stateMetadata);
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                "tech.streamfusion.flink.planner.aggregate.StreamFusionGlobalGroupAggregateTranslator",
                new Class<?>[] {
                    RowType.class, RowType.class, RowType.class, int.class, AggregateCall[].class,
                    boolean[].class, boolean.class, boolean.class, long.class, ReadableConfig.class
                },
                originalInputType,
                getInputEdges().get(0).getOutputType(),
                getOutputType(),
                groupingCount,
                calls,
                retractable,
                generateUpdateBefore,
                needRetraction,
                retention,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
