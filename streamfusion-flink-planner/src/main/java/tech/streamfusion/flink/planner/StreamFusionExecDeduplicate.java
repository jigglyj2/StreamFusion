/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for timer-free rowtime keep-last deduplication. */
public final class StreamFusionExecDeduplicate extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateTranslator";

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
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.uniqueKeys = uniqueKeys.clone();
        this.isRowtime = isRowtime;
        this.keepLastRow = keepLastRow;
        this.outputInsertOnly = outputInsertOnly;
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateMetadata = stateMetadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        long stateRetention = StateMetadata.getStateTtlForOneInputOperator(config, stateMetadata);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class);
            RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                    planner.getFlinkContext().getClassLoader(),
                    uniqueKeys,
                    org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of((RowType) inputEdge.getOutputType()));
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    (RowType) inputEdge.getOutputType(),
                    (RowType) getOutputType(),
                    uniqueKeys,
                    isRowtime,
                    keepLastRow,
                    outputInsertOnly,
                    generateUpdateBefore,
                    stateRetention,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion Deduplicate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion Deduplicate runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion Deduplicate translation failed", e.getCause());
        }
    }
}
