/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for window-scoped first/last-row deduplication. */
public final class StreamFusionExecWindowDeduplicate extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.window.StreamFusionWindowDeduplicateTranslator";

    private final int[] partitionKeys;
    private final int orderKey;
    private final boolean keepLast;
    private final WindowingStrategy windowing;

    public StreamFusionExecWindowDeduplicate(
            ReadableConfig config,
            int[] partitionKeys,
            int orderKey,
            boolean keepLast,
            WindowingStrategy windowing,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-window-deduplicate_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionKeys = partitionKeys.clone();
        this.orderKey = orderKey;
        this.keepLast = keepLast;
        this.windowing = windowing;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        RowType inputType = (RowType) edge.getOutputType();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), partitionKeys, InternalTypeInfo.of(inputType));
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
                    int.class,
                    boolean.class,
                    WindowingStrategy.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    partitionKeys,
                    orderKey,
                    keepLast,
                    windowing,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion WindowDeduplicate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion WindowDeduplicate runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion WindowDeduplicate translation failed", e.getCause());
        }
    }
}
