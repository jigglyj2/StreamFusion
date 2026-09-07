/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
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
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Selected regular-join fragment; the common region owns execution, routing and state. */
public final class StreamFusionExecRegularJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    private final JoinSpec joinSpec;
    private final List<int[]> leftUpsertKeys;
    private final List<int[]> rightUpsertKeys;
    private final long leftStateTtlMillis;
    private final long rightStateTtlMillis;

    public StreamFusionExecRegularJoin(
            ReadableConfig config,
            JoinSpec joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            long leftStateTtlMillis,
            long rightStateTtlMillis,
            InputProperty leftInput,
            InputProperty rightInput,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-regular-join_1"),
                config,
                List.of(leftInput, rightInput),
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.leftUpsertKeys = leftUpsertKeys;
        this.rightUpsertKeys = rightUpsertKeys;
        this.leftStateTtlMillis = leftStateTtlMillis;
        this.rightStateTtlMillis = rightStateTtlMillis;
    }

    @Override
    public boolean ownsNativeKeyedState() {
        return true;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                "tech.streamfusion.flink.join.StreamFusionRegularJoinTranslator",
                new Class<?>[] {
                    RowType.class,
                    RowType.class,
                    RowType.class,
                    JoinSpec.class,
                    List.class,
                    List.class,
                    long.class,
                    long.class,
                    ReadableConfig.class
                },
                getInputEdges().get(0).getOutputType(),
                getInputEdges().get(1).getOutputType(),
                getOutputType(),
                joinSpec,
                leftUpsertKeys,
                rightUpsertKeys,
                leftStateTtlMillis,
                rightStateTtlMillis,
                getPersistedConfig());
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
