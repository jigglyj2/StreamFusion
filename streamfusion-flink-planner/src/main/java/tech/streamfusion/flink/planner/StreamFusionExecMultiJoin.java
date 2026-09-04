/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Distinct StreamFusion physical node for N-input streaming join. */
public final class StreamFusionExecMultiJoin extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.join.StreamFusionMultiJoinTranslator";

    private final List<FlinkJoinType> joinTypes;
    private final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;
    private final List<List<int[]>> inputUniqueKeys;
    private final long[] stateRetentionMillis;
    private final boolean equiOnly;

    public StreamFusionExecMultiJoin(
            ReadableConfig config,
            List<FlinkJoinType> joinTypes,
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            List<List<int[]>> inputUniqueKeys,
            long[] stateRetentionMillis,
            boolean equiOnly,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                new ExecNodeContext("streamfusion-exec-multi-join_1"),
                config,
                inputProperties,
                outputType,
                description);
        this.joinTypes = List.copyOf(joinTypes);
        this.joinAttributeMap = Map.copyOf(joinAttributeMap);
        this.inputUniqueKeys = List.copyOf(inputUniqueKeys);
        this.stateRetentionMillis = stateRetentionMillis.clone();
        this.equiOnly = equiOnly;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        List<Transformation<RowData>> inputs = new ArrayList<>(getInputEdges().size());
        List<RowType> inputTypes = new ArrayList<>(getInputEdges().size());
        for (ExecEdge edge : getInputEdges()) {
            inputs.add((Transformation<RowData>) edge.translateToPlan(planner));
            inputTypes.add((RowType) edge.getOutputType());
        }
        AttributeBasedJoinKeyExtractor extractor = new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputTypes);
        List<int[]> commonKeyIndices = IntStream.range(0, inputTypes.size())
                .mapToObj(extractor::getCommonJoinKeyIndices)
                .collect(Collectors.toList());
        List<RowDataKeySelector> commonSelectors = new ArrayList<>(inputTypes.size());
        List<Map<Integer, RowDataKeySelector>> conditionSelectors = new ArrayList<>(inputTypes.size());
        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
        for (int input = 0; input < inputTypes.size(); input++) {
            InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(inputTypes.get(input));
            commonSelectors.add(KeySelectorUtil.getRowDataSelector(classLoader, commonKeyIndices.get(input), typeInfo));
            Set<Integer> fields = new LinkedHashSet<>();
            for (List<ConditionAttributeRef> conditions : joinAttributeMap.values()) {
                for (ConditionAttributeRef condition : conditions) {
                    if (condition.leftInputId == input) {
                        fields.add(condition.leftFieldIndex);
                    }
                    if (condition.rightInputId == input) {
                        fields.add(condition.rightFieldIndex);
                    }
                }
            }
            Map<Integer, RowDataKeySelector> selectors = new LinkedHashMap<>();
            for (int field : fields) {
                selectors.put(field, KeySelectorUtil.getRowDataSelector(classLoader, new int[] {field}, typeInfo));
            }
            conditionSelectors.add(selectors);
        }
        try {
            Class<?> translator = Class.forName(TRANSLATOR_CLASS, true, classLoader);
            Method method = translator.getMethod(
                    "translate",
                    List.class,
                    List.class,
                    RowType.class,
                    List.class,
                    List.class,
                    Map.class,
                    List.class,
                    long[].class,
                    boolean.class,
                    ReadableConfig.class,
                    StreamExecutionEnvironment.class,
                    List.class,
                    List.class);
            Transformation<RowData> result = (Transformation<RowData>) method.invoke(
                    null,
                    inputs,
                    inputTypes,
                    (RowType) getOutputType(),
                    commonKeyIndices,
                    joinTypes,
                    joinAttributeMap,
                    inputUniqueKeys,
                    stateRetentionMillis,
                    equiOnly,
                    getPersistedConfig(),
                    planner.getExecEnv(),
                    commonSelectors,
                    conditionSelectors);
            if (result == null) {
                throw new IllegalStateException("A selected StreamFusion multi-join failed translation");
            }
            return result;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException failure) {
            throw new IllegalStateException("Could not invoke the StreamFusion multi-join runtime", failure);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("StreamFusion multi-join translation failed", failure.getCause());
        }
    }
}
