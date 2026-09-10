/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.List;
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
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for event-time Window Join. */
public final class StreamFusionExecWindowJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.planner.window.StreamFusionWindowJoinTranslator";

    private final JoinSpec joinSpec;
    private final WindowingStrategy leftWindowing;
    private final WindowingStrategy rightWindowing;

    public StreamFusionExecWindowJoin(
            ReadableConfig config,
            JoinSpec joinSpec,
            WindowingStrategy leftWindowing,
            WindowingStrategy rightWindowing,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-join_1"),
                config,
                List.of(leftInput, rightInput),
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.leftWindowing = leftWindowing;
        this.rightWindowing = rightWindowing;
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
                    RowType.class,
                    JoinSpec.class,
                    WindowingStrategy.class,
                    WindowingStrategy.class,
                    ReadableConfig.class
                },
                getInputEdges().get(0).getOutputType(),
                getInputEdges().get(1).getOutputType(),
                getOutputType(),
                joinSpec,
                leftWindowing,
                rightWindowing,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
