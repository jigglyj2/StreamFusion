/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.nativebridge.NativeExchangeOutputs;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

/** Native output routing belongs to the producing plan; Flink side outputs own its transport edges. */
public final class NativeRegionExchangeOutputs implements Serializable {
    private final List<Binding> bindings = new ArrayList<>();

    NativeRegionExchangeOutputs() {}

    NativeRegionExchangeOutputs(NativeRegionExchangeOutputs source) {
        bindings.addAll(source.bindings);
    }

    static final class Binding implements Serializable {
        final int port;
        final byte[] plan;

        Binding(int port, byte[] plan) {
            this.port = port;
            this.plan = plan.clone();
        }
    }

    int bind(int port, byte[] plan) {
        for (int id = 0; id < bindings.size(); id++) {
            var binding = bindings.get(id);
            if (binding.port == port && Arrays.equals(binding.plan, plan)) return id;
        }
        bindings.add(new Binding(port, plan));
        return bindings.size() - 1;
    }

    static OutputTag<NativeExchangeFrame> tag(int id) {
        return new OutputTag<>("streamfusion-native-exchange-" + id, NativeExchangeFrameTypeInfo.INSTANCE);
    }
    /** Returns null when this boundary still needs the Java source/key adapter. */
    public static Transformation<NativeExchangeFrame> frame(Transformation<RowData> input, RowType type, byte[] plan) {
        try {
            var exchange = NativeExchangePlan.parseFrom(plan);
            if (exchange.getMetadataColumns().hasRoutingKeyIndex() || exchange.getTransportRoutingKey()) return null;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native exchange plan", failure);
        }
        Transformation<?> owner = input;
        OutputTag<?> selected = null;
        if (owner instanceof SideOutputTransformation) {
            selected = ((SideOutputTransformation<?>) owner).getOutputTag();
            owner = owner.getInputs().get(0);
        }
        Object candidate;
        if (owner instanceof OneInputTransformation)
            candidate = ((OneInputTransformation<?, ?>) owner).getOperatorFactory();
        else if (owner instanceof MultipleInputTransformation)
            candidate = ((MultipleInputTransformation<?>) owner).getOperatorFactory();
        else return null;
        if (!(candidate instanceof StreamFusionNativeRegionOperatorFactory)) return null;
        var factory = (StreamFusionNativeRegionOperatorFactory) candidate;
        int port = factory.outputPort(selected);
        if (port < 0) return null;
        int id = factory.bindFrameOutput(port, type, plan);
        var frames = new SideOutputTransformation<>(owner, tag(id));
        if (owner.getMaxParallelism() > 0) frames.setMaxParallelism(owner.getMaxParallelism());
        return frames;
    }

    /** Inspect the finalized Flink edges at task open, so true mixed consumers keep Arrow output. */
    Runtime open(StreamConfig config, ClassLoader loader, int ports, StreamFusionTaskMemory memory) {
        if (bindings.isEmpty()) return new Runtime(new boolean[0]);
        boolean[] arrow = new boolean[ports];
        Arrays.fill(arrow, true);
        for (var binding : bindings) arrow[binding.port] = false;
        var tags = new ArrayList<OutputTag<?>>();
        config.getChainedOutputs(loader).forEach(edge -> tags.add(edge.getOutputTag()));
        config.getOperatorNonChainedOutputs(loader).forEach(edge -> tags.add(edge.getOutputTag()));
        for (var tag : tags) {
            if (tag == null) arrow[0] = true;
            else
                for (int port = 1; port < ports; port++)
                    if (NativeSharedRegionOutputs.tag(port).equals(tag)) arrow[port] = true;
        }
        int[] sources = bindings.stream().mapToInt(binding -> binding.port).toArray();
        List<byte[]> plans = new ArrayList<>();
        for (var binding : bindings) plans.add(binding.plan);
        NativeExchangeOutputs.bind(memory.executionContext(), sources, plans, arrow, memory.nativeMemoryManager());
        boolean[] counted = arrow.clone();
        boolean[] logical = new boolean[bindings.size()];
        for (int id = 0; id < bindings.size(); id++) {
            int port = bindings.get(id).port;
            logical[id] = !counted[port];
            counted[port] = true;
        }
        return new Runtime(logical);
    }

    static final class Runtime {
        private final boolean[] logical;

        Runtime(boolean[] logical) {
            this.logical = logical;
        }

        boolean countsRows(int binding) {
            return logical[binding];
        }
    }
}
