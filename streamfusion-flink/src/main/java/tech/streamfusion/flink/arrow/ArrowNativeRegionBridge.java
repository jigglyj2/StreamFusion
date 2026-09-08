/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeRegionStream;

/** Arrow edge for one native owner with multiple, independently typed exits. */
public final class ArrowNativeRegionBridge {
    private final NativeExecutionContext context;
    private final List<RowType> outputTypes;
    private final BufferAllocator allocator;
    private final NativePlanInputs inputEdge = new NativePlanInputs(true);
    private final Schema[] outputSchemas;

    public ArrowNativeRegionBridge(
            NativeExecutionContext context, List<RowType> outputTypes, BufferAllocator allocator) {
        if (!context.hasRegionOutputs() || !context.requiresInputEnvelope())
            throw new IllegalArgumentException("Native region edge requires an owned-envelope region context");
        if (outputTypes.isEmpty()) throw new IllegalArgumentException("Native region requires output ports");
        this.context = context;
        this.outputTypes = List.copyOf(outputTypes);
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        outputSchemas = new Schema[outputTypes.size()];
    }

    public ArrowNativeRegionOutput executeStream(List<ArrowRowDataBatch> inputs) {
        return execute(inputs, null);
    }

    public ArrowNativeRegionOutput executeControlStream(List<ArrowRowDataBatch> inputs, byte[] controls) {
        Objects.requireNonNull(controls, "controls");
        if (inputs.stream().anyMatch(input -> input.size() != 0))
            throw new IllegalArgumentException("Native control invocation requires empty input batches");
        return execute(inputs, controls);
    }

    private ArrowNativeRegionOutput execute(List<ArrowRowDataBatch> inputs, byte[] controls) {
        try (var prepared = inputEdge.prepare(inputs)) {
            var stream = NativeRegionStream.open(context, prepared.arrayAddresses, prepared.schemaAddresses, controls);
            try {
                var output = new ArrowNativeRegionOutput(stream, outputTypes, allocator, outputSchemas);
                output.ownInputs(prepared.transferToOutput());
                return output;
            } catch (RuntimeException | Error failure) {
                stream.close();
                throw failure;
            }
        }
    }
}
