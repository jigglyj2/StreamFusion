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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Selected timer-free deduplication fragment; common region infrastructure binds its state. */
public final class StreamFusionExecDeduplicate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, StreamFusionNativePlanNode {
    private final StreamFusionNativeNodeMetadata nativeMetadata = new StreamFusionNativeNodeMetadata();

    @Override
    public StreamFusionNativeNodeMetadata nativeMetadata() {
        return nativeMetadata;
    }

    private final int[] uniqueKeys;
    private final boolean isRowtime;
    private final boolean keepLastRow;
    private final boolean outputInsertOnly;
    private final boolean generateUpdateBefore;
    private final List<StateMetadata> stateMetadata;

    public StreamFusionExecDeduplicate(
            ReadableConfig persistedConfig,
            int[] uniqueKeys,
            boolean isRowtime,
            boolean keepLastRow,
            boolean outputInsertOnly,
            boolean generateUpdateBefore,
            List<StateMetadata> stateMetadata,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-deduplicate_1"),
                persistedConfig,
                List.of(inputProperty),
                outputType,
                description);
        this.uniqueKeys = uniqueKeys.clone();
        this.isRowtime = isRowtime;
        this.keepLastRow = keepLastRow;
        this.outputInsertOnly = outputInsertOnly;
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateMetadata = stateMetadata;
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
                "tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateTranslator",
                new Class<?>[] {
                    RowType.class,
                    RowType.class,
                    int[].class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class
                },
                getInputEdges().get(0).getOutputType(),
                getOutputType(),
                uniqueKeys,
                isRowtime,
                keepLastRow,
                outputInsertOnly,
                generateUpdateBefore,
                retention,
                config);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        return StreamFusionStatelessRegion.translate(this, planner);
    }
}
