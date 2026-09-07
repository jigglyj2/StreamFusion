/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.Collections;
import org.apache.calcite.rex.RexCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

/** Distinct accelerator exec node for Flink's internal set-operation row replicator. */
public final class StreamFusionExecReplicateRows extends CommonExecCorrelate
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.replicate.StreamFusionReplicateRowsTranslator";

    private final RexCall streamFusionInvocation;
    private final FlinkJoinType streamFusionJoinType;

    public StreamFusionExecReplicateRows(
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-replicate-rows_1"),
                persistedConfig,
                joinType,
                invocation,
                null,
                TableStreamOperator.class,
                true,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.streamFusionInvocation = invocation;
        this.streamFusionJoinType = joinType;
    }

    RexCall streamFusionInvocation() {
        return streamFusionInvocation;
    }

    FlinkJoinType streamFusionJoinType() {
        return streamFusionJoinType;
    }

    @Override
    public byte[] nativePlanFragment(PlannerBase planner) {
        return StreamFusionNativePlanNode.invokeBuilder(
                planner,
                TRANSLATOR_CLASS,
                new Class<?>[] {RowType.class, RowType.class, Object.class, Object.class},
                (RowType) getInputEdges().get(0).getOutputType(),
                (RowType) getOutputType(),
                streamFusionJoinType,
                streamFusionInvocation);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
