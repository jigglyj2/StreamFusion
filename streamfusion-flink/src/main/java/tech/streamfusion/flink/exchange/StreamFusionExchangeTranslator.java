/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Builds the Flink-owned runtime topology around the native Arrow exchange data path. */
public final class StreamFusionExchangeTranslator {
    private StreamFusionExchangeTranslator() {}

    public static Transformation<RowData> hash(
            Transformation<RowData> input, RowType rowType, int[] keys, int maxParallelism) {
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, keys, maxParallelism);
        return translate(input, rowType, plan, new NativeExchangePartitioner(maxParallelism), false);
    }

    public static Transformation<RowData> singleton(Transformation<RowData> input, RowType rowType) {
        byte[] plan = NativeExchangePlanSerializer.singleton(rowType);
        return translate(input, rowType, plan, new GlobalPartitioner<>(), true);
    }

    private static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType rowType,
            byte[] plan,
            StreamPartitioner<NativeExchangeFrame> partitioner,
            boolean singleton) {
        OneInputTransformation<ArrowRowDataBatch, NativeExchangeFrame> writer = new OneInputTransformation<>(
                StreamFusionArrowBoundaries.toArrow(input, rowType),
                "StreamFusionExchangeWriter",
                SimpleOperatorFactory.of(new NativeExchangeWriterOperator(rowType, plan)),
                NativeExchangeFrameTypeInfo.INSTANCE,
                input.getParallelism());
        writer.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        PartitionTransformation<NativeExchangeFrame> exchange = new PartitionTransformation<>(writer, partitioner);
        exchange.setOutputType(NativeExchangeFrameTypeInfo.INSTANCE);
        exchange.setParallelism(singleton ? 1 : ExecutionConfig.PARALLELISM_DEFAULT);
        OneInputTransformation<NativeExchangeFrame, ArrowRowDataBatch> reader = new OneInputTransformation<>(
                exchange,
                "StreamFusionExchangeReader",
                SimpleOperatorFactory.of(new NativeExchangeReaderOperator(rowType, plan)),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                exchange.getParallelism());
        reader.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        reader.setOutputType(ArrowRowDataBatchTypeInfo.INSTANCE);
        return StreamFusionArrowBoundaries.asPlannerTransformation(reader);
    }
}
