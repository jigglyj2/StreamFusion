/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.watermark;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Creates the Arrow-native watermark assigner with Flink's generated expression. */
final class StreamFusionArrowWatermarkAssignerOperatorFactory extends AbstractStreamOperatorFactory<ArrowRowDataBatch>
        implements OneInputStreamOperatorFactory<ArrowRowDataBatch, ArrowRowDataBatch> {
    private final int rowtimeFieldIndex;
    private final long idleTimeout;
    private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

    StreamFusionArrowWatermarkAssignerOperatorFactory(
            int rowtimeFieldIndex, long idleTimeout, GeneratedWatermarkGenerator generatedWatermarkGenerator) {
        this.rowtimeFieldIndex = rowtimeFieldIndex;
        this.idleTimeout = idleTimeout;
        this.generatedWatermarkGenerator = generatedWatermarkGenerator;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<ArrowRowDataBatch>> T createStreamOperator(
            StreamOperatorParameters<ArrowRowDataBatch> parameters) {
        WatermarkGenerator generator = generatedWatermarkGenerator.newInstance(
                parameters.getContainingTask().getUserCodeClassLoader());
        return (T) new StreamFusionArrowWatermarkAssignerOperator(
                parameters, rowtimeFieldIndex, generator, idleTimeout, processingTimeService);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return StreamFusionArrowWatermarkAssignerOperator.class;
    }
}
