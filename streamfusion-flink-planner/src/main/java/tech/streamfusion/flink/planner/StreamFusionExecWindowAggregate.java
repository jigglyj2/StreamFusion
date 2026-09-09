/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.Collections;
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

/** Distinct StreamFusion physical node for SQL window aggregation. */
public final class StreamFusionExecWindowAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.planner.window.StreamFusionSessionWindowAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] aggregateCalls;
    private final WindowingStrategy windowing;
    private final NamedWindowProperty[] namedWindowProperties;
    private final boolean needRetraction;

    public StreamFusionExecWindowAggregate(
            ReadableConfig persistedConfig,
            int[] grouping,
            AggregateCall[] aggregateCalls,
            WindowingStrategy windowing,
            NamedWindowProperty[] namedWindowProperties,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-aggregate_1"),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping.clone();
        this.aggregateCalls = aggregateCalls.clone();
        this.windowing = windowing;
        this.namedWindowProperties = namedWindowProperties.clone();
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
                TRANSLATOR_CLASS,
                new Class<?>[] {
                    RowType.class,
                    RowType.class,
                    int[].class,
                    AggregateCall[].class,
                    WindowingStrategy.class,
                    NamedWindowProperty[].class,
                    boolean.class,
                    ReadableConfig.class
                },
                getInputEdges().get(0).getOutputType(),
                getOutputType(),
                grouping,
                aggregateCalls,
                windowing,
                namedWindowProperties,
                needRetraction,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
