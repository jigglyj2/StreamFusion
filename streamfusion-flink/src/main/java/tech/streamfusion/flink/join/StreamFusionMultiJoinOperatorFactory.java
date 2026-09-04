/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Serializable V2 factory for the native N-input join. */
final class StreamFusionMultiJoinOperatorFactory extends AbstractStreamOperatorFactory<ArrowRowDataBatch> {
    private final List<RowType> inputTypes;
    private final RowType outputType;
    private final List<int[]> commonKeyFields;
    private final byte[] plan;
    private final List<RowDataKeySelector> commonKeySelectors;
    private final List<Map<Integer, RowDataKeySelector>> conditionSelectors;
    private final List<byte[]> exchangePlans;

    StreamFusionMultiJoinOperatorFactory(
            List<RowType> inputTypes,
            RowType outputType,
            List<int[]> commonKeyFields,
            byte[] plan,
            List<RowDataKeySelector> commonKeySelectors,
            List<Map<Integer, RowDataKeySelector>> conditionSelectors,
            List<byte[]> exchangePlans) {
        this.inputTypes = List.copyOf(inputTypes);
        this.outputType = outputType;
        this.commonKeyFields = List.copyOf(commonKeyFields);
        this.plan = plan.clone();
        this.commonKeySelectors = List.copyOf(commonKeySelectors);
        this.conditionSelectors = List.copyOf(conditionSelectors);
        this.exchangePlans = List.copyOf(exchangePlans);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<ArrowRowDataBatch>> T createStreamOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters) {
        return (T) new StreamFusionArrowMultiJoinOperator(
                parameters,
                inputTypes,
                outputType,
                commonKeyFields,
                plan,
                commonKeySelectors,
                conditionSelectors,
                exchangePlans);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return StreamFusionArrowMultiJoinOperator.class;
    }
}
