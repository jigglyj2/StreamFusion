/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for timer-free unbounded keyed aggregation. */
public final class StreamFusionExecGroupAggregate extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator";

    private final int[] grouping;
    private final AggregateCall[] aggregateCalls;
    private final boolean[] aggregateCallNeedRetractions;
    private final boolean generateUpdateBefore;
    private final boolean needRetraction;
    private final List<StateMetadata> stateMetadata;

    public StreamFusionExecGroupAggregate(
            ReadableConfig persistedConfig,
            int[] grouping,
            AggregateCall[] aggregateCalls,
            boolean[] aggregateCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            List<StateMetadata> stateMetadata,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-group-aggregate_1"),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.grouping = grouping.clone();
        this.aggregateCalls = aggregateCalls.clone();
        this.aggregateCallNeedRetractions = aggregateCallNeedRetractions.clone();
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
        this.stateMetadata = stateMetadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        long stateRetention = StateMetadata.getStateTtlForOneInputOperator(config, stateMetadata);
        RowType inputType = (RowType) inputEdge.getOutputType();
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputType));
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "translate",
                    Transformation.class,
                    RowType.class,
                    RowType.class,
                    int[].class,
                    AggregateCall[].class,
                    boolean[].class,
                    boolean.class,
                    boolean.class,
                    long.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    grouping,
                    aggregateCalls,
                    aggregateCallNeedRetractions,
                    generateUpdateBefore,
                    needRetraction,
                    stateRetention,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion GroupAggregate failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion GroupAggregate runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion GroupAggregate translation failed", e.getCause());
        }
    }
}
