/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionExchangeTranslatorTest {
    @Test
    void buildsHashExchangeInsideFlinksTransformationTopology() {
        Transformation<RowData> input = input();

        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>)
                StreamFusionExchangeTranslator.hash(input, RowType.of(new IntType(false)), new int[] {0}, 128);
        PartitionTransformation<?> exchange =
                (PartitionTransformation<?>) reader.getInputs().get(0);
        OneInputTransformation<?, ?> writer =
                (OneInputTransformation<?, ?>) exchange.getInputs().get(0);

        assertThat(writer.getName()).isEqualTo("StreamFusionExchangeWriter");
        assertThat(writer.getOutputType()).isEqualTo(NativeExchangeFrameTypeInfo.INSTANCE);
        assertThat(exchange.getPartitioner()).isInstanceOf(NativeExchangePartitioner.class);
        assertThat(exchange.getParallelism()).isEqualTo(ExecutionConfig.PARALLELISM_DEFAULT);
        assertThat(reader.getName()).isEqualTo("StreamFusionExchangeReader");
    }

    @Test
    void keepsSingletonSchedulingOnFlinksGlobalPartitioner() {
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>)
                StreamFusionExchangeTranslator.singleton(input(), RowType.of(new IntType(false)));
        PartitionTransformation<?> exchange =
                (PartitionTransformation<?>) reader.getInputs().get(0);

        assertThat(exchange.getPartitioner()).isInstanceOf(GlobalPartitioner.class);
        assertThat(exchange.getParallelism()).isEqualTo(1);
        assertThat(reader.getParallelism()).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    private static Transformation<RowData> input() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        return (Transformation<RowData>) (Transformation<?>) environment
                .fromData((RowData) org.apache.flink.table.data.GenericRowData.of(1))
                .returns(InternalTypeInfo.of(RowType.of(new IntType(false))))
                .getTransformation();
    }
}
