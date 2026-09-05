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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.types.logical.RowType;

/** Bounded native exchange retaining Flink-owned network transport and key grouping. */
public final class StreamFusionBatchExecExchange extends CommonExecExchange implements BatchExecNode<RowData> {
    private static final String TRANSLATOR = "tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator";

    public StreamFusionBatchExecExchange(
            ReadableConfig config, InputProperty inputProperty, RowType outputType, String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-batch-exec-exchange_1"),
                config,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge edge = getInputEdges().get(0);
        Transformation<RowData> input = (Transformation<RowData>) edge.translateToPlan(planner);
        InputProperty.RequiredDistribution distribution =
                getInputProperties().get(0).getRequiredDistribution();
        try {
            Class<?> translator =
                    Class.forName(TRANSLATOR, true, planner.getFlinkContext().getClassLoader());
            switch (distribution.getType()) {
                case UNKNOWN:
                    // UNKNOWN is Flink's placeholder for local bounded operators and carries no
                    // distribution requirement. Preserve the already-vectorized input directly.
                    return input;
                case KEEP_INPUT_AS_IS:
                    // The input edge already has the required distribution. Flink's bounded
                    // KeepInputAsIs exchange exists to retain that partitioning through a
                    // consecutive operator chain, so preserving the native Arrow transformation
                    // is the exact zero-copy implementation.
                    return input;
                case HASH:
                    Method hash = translator.getMethod(
                            "hash",
                            Transformation.class,
                            RowType.class,
                            int[].class,
                            int.class,
                            int.class,
                            boolean.class,
                            boolean.class);
                    return (Transformation<RowData>) hash.invoke(
                            null,
                            input,
                            (RowType) getOutputType(),
                            ((HashDistribution) distribution).getKeys(),
                            DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                            planner.getExecEnv().getParallelism(),
                            false,
                            true);
                case SINGLETON:
                    Method singleton = translator.getMethod("singleton", Transformation.class, RowType.class);
                    return (Transformation<RowData>) singleton.invoke(null, input, (RowType) getOutputType());
                default:
                    throw new IllegalStateException(
                            "Selected unsupported bounded native exchange " + distribution.getType());
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the bounded native exchange", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Bounded native exchange translation failed", failure.getCause());
        }
    }
}
