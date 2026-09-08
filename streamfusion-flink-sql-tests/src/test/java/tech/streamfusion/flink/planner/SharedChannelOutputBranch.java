/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

/** Adds a second real sink adapter branch to Flink's otherwise linear task test chainer. */
final class SharedChannelOutputBranch {
    private SharedChannelOutputBranch() {}

    static ResultPartitionWriter attach(
            StreamConfig head, RowType outputType, Queue<Object> output, org.apache.flink.util.OutputTag<?> tag)
            throws Exception {
        var loader = SharedChannelOutputBranch.class.getClassLoader();
        var configs = head.getTransitiveChainedTaskConfigs(loader);
        var constructor = Class.forName("tech.streamfusion.flink.arrow.ArrowBatchToRowDataOperator")
                .getDeclaredConstructor();
        constructor.setAccessible(true);
        @SuppressWarnings("unchecked")
        var adapter = (StreamOperator<RowData>) constructor.newInstance();
        var side = new StreamConfig(new Configuration());
        side.setStreamOperatorFactory(SimpleOperatorFactory.of(adapter));
        side.setOperatorID(new OperatorID(47, 53));
        side.setChainIndex(2);
        side.setChainEnd();
        side.setupNetworkInputs(ArrowRowDataBatchSerializer.INSTANCE);
        side.setTypeSerializerOut(new RowDataSerializer(outputType));
        var network = new NonChainedOutput(
                true,
                2,
                1,
                1,
                100,
                false,
                new IntermediateDataSetID(),
                null,
                new BroadcastPartitioner<>(),
                ResultPartitionType.PIPELINED_BOUNDED);
        side.setNumberOfOutputs(1);
        side.setOperatorNonChainedOutputs(List.of(network));
        side.setVertexNonChainedOutputs(List.of(network));
        side.serializeAllConfigs();
        configs.put(2, side);
        var edge = new StreamEdge(
                new StreamNode(0, null, null, (StreamOperator<?>) null, null, null),
                new StreamNode(2, null, null, (StreamOperator<?>) null, null, null),
                0,
                null,
                tag);
        var chained = new ArrayList<>(head.getChainedOutputs(loader));
        chained.add(edge);
        head.setChainedOutputs(chained);
        head.setTypeSerializerSideOut(tag, ArrowRowDataBatchSerializer.INSTANCE);
        var networks = new ArrayList<>(head.getVertexNonChainedOutputs(loader));
        networks.add(network);
        head.setVertexNonChainedOutputs(networks);
        head.setAndSerializeTransitiveChainedTaskConfigs(configs);
        head.serializeAllConfigs();
        return new RecordOrEventCollectingResultPartitionWriter<>(
                output, new StreamElementSerializer<>(new RowDataSerializer(outputType)));
    }
}
