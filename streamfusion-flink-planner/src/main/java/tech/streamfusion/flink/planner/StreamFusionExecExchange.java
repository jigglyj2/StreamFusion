/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.HashDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical exchange node that retains Flink's runtime topology. */
public final class StreamFusionExecExchange extends CommonExecExchange implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator";

    public StreamFusionExecExchange(
            ReadableConfig persistedConfig, InputProperty inputProperty, RowType outputType, String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-exchange_1"),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        InputProperty.RequiredDistribution distribution =
                getInputProperties().get(0).getRequiredDistribution();
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS, true, planner.getFlinkContext().getClassLoader());
            switch (distribution.getType()) {
                case HASH:
                    Method hash =
                            translator.getMethod("hash", Transformation.class, RowType.class, int[].class, int.class);
                    return (Transformation<RowData>) hash.invoke(
                            null,
                            input,
                            (RowType) getOutputType(),
                            ((HashDistribution) distribution).getKeys(),
                            DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
                case SINGLETON:
                    Method singleton = translator.getMethod("singleton", Transformation.class, RowType.class);
                    return (Transformation<RowData>) singleton.invoke(null, input, (RowType) getOutputType());
                default:
                    throw new IllegalStateException("Selected unsupported native exchange " + distribution.getType());
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion exchange runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion exchange translation failed", e.getCause());
        }
    }
}
