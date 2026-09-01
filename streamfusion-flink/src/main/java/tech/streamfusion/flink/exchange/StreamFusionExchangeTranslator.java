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
        return hash(input, rowType, keys, maxParallelism, 1, true);
    }

    public static Transformation<RowData> hash(
            Transformation<RowData> input,
            RowType rowType,
            int[] keys,
            int maxParallelism,
            int parallelism,
            boolean preserveKeyGroups) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Native hash exchange parallelism must be positive");
        }
        byte[] plan = NativeExchangePlanSerializer.hash(rowType, keys, maxParallelism, parallelism, preserveKeyGroups);
        return translate(input, rowType, keys, plan, new NativeExchangePartitioner(maxParallelism), false);
    }

    public static Transformation<RowData> singleton(Transformation<RowData> input, RowType rowType) {
        byte[] plan = NativeExchangePlanSerializer.singleton(rowType);
        return translate(input, rowType, new int[0], plan, new GlobalPartitioner<>(), true);
    }

    private static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType rowType,
            int[] keys,
            byte[] plan,
            StreamPartitioner<NativeExchangeFrame> partitioner,
            boolean singleton) {
        OneInputTransformation<ArrowRowDataBatch, NativeExchangeFrame> writer = new OneInputTransformation<>(
                StreamFusionArrowBoundaries.toArrow(input, rowType),
                "StreamFusionExchangeWriter",
                SimpleOperatorFactory.of(new NativeExchangeWriterOperator(rowType, keys, plan)),
                NativeExchangeFrameTypeInfo.INSTANCE,
                input.getParallelism());
        int upstreamMaxParallelism = inheritedMaxParallelism(input);
        if (upstreamMaxParallelism > 0) {
            // The writer must remain chained to its raw Arrow producer. Only the framed output
            // below is legal on a Flink network edge.
            writer.setMaxParallelism(upstreamMaxParallelism);
        }
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

    private static int inheritedMaxParallelism(Transformation<?> transformation) {
        if (transformation.getMaxParallelism() > 0) {
            return transformation.getMaxParallelism();
        }
        int inherited = ExecutionConfig.PARALLELISM_DEFAULT;
        for (Transformation<?> input : transformation.getInputs()) {
            int candidate = inheritedMaxParallelism(input);
            if (candidate <= 0) {
                continue;
            }
            if (inherited > 0 && inherited != candidate) {
                return ExecutionConfig.PARALLELISM_DEFAULT;
            }
            inherited = candidate;
        }
        return inherited;
    }
}
