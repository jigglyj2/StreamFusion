/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for keyed changelog normalization. */
public final class StreamFusionExecChangelogNormalize extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS =
            "tech.streamfusion.flink.changelog.StreamFusionChangelogNormalizeTranslator";

    private final int[] uniqueKeys;
    private final boolean generateUpdateBefore;
    private final RexNode filterCondition;
    private final List<StateMetadata> stateMetadata;

    public StreamFusionExecChangelogNormalize(
            ReadableConfig persistedConfig,
            int[] uniqueKeys,
            boolean generateUpdateBefore,
            @Nullable RexNode filterCondition,
            List<StateMetadata> stateMetadata,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-changelog-normalize_1"),
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.uniqueKeys = uniqueKeys.clone();
        this.generateUpdateBefore = generateUpdateBefore;
        this.filterCondition = filterCondition;
        this.stateMetadata = stateMetadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        RowType inputType = (RowType) inputEdge.getOutputType();
        Transformation<RowData> input = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        long stateRetention = StateMetadata.getStateTtlForOneInputOperator(config, stateMetadata);
        GeneratedFilterCondition generatedFilter = filterCondition == null
                ? null
                : FilterCodeGenerator.generateFilterCondition(
                        config, planner.getFlinkContext().getClassLoader(), filterCondition, inputType);
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
                    long.class,
                    GeneratedFilterCondition.class,
                    ReadableConfig.class,
                    org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class,
                    RowDataKeySelector.class);
            RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                    planner.getFlinkContext().getClassLoader(),
                    uniqueKeys,
                    org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(inputType));
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    input,
                    inputType,
                    (RowType) getOutputType(),
                    uniqueKeys,
                    generateUpdateBefore,
                    stateRetention,
                    generatedFilter,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    selector);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion ChangelogNormalize failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not invoke the StreamFusion ChangelogNormalize runtime", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion ChangelogNormalize translation failed", e.getCause());
        }
    }
}
