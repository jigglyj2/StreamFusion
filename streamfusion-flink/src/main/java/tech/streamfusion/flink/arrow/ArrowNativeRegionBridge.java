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
    private final NativePlanInputs inputEdge;
    private final Schema[] outputSchemas;

    public ArrowNativeRegionBridge(
            NativeExecutionContext context, List<RowType> outputTypes, BufferAllocator allocator) {
        this(context, outputTypes, allocator, List.of(), null);
    }

    ArrowNativeRegionBridge(
            NativeExecutionContext context,
            List<RowType> outputTypes,
            BufferAllocator allocator,
            List<Integer> clockPorts,
            java.util.function.LongSupplier clock) {
        inputEdge = new NativePlanInputs(true, clockPorts, clock);
        if (!context.hasRegionOutputs() || !context.requiresInputEnvelope())
            throw new IllegalArgumentException("Native region edge requires an owned-envelope region context");
        if (outputTypes.isEmpty()) throw new IllegalArgumentException("Native region requires output ports");
        this.context = context;
        this.outputTypes = List.copyOf(outputTypes);
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        outputSchemas = new Schema[outputTypes.size()];
    }

    public ArrowNativeRegionOutput executeStream(List<ArrowRowDataBatch> inputs) {
        return execute(inputs, (byte[]) null);
    }

    public ArrowNativeRegionOutput executeControlStream(List<ArrowRowDataBatch> inputs, byte[] controls) {
        Objects.requireNonNull(controls, "controls");
        if (inputs.stream().anyMatch(input -> input.size() != 0))
            throw new IllegalArgumentException("Native control invocation requires empty input batches");
        return execute(inputs, controls);
    }

    public ArrowNativeRegionOutput executeExchangeStream(
            List<ArrowRowDataBatch> emptyInputs,
            int port,
            byte[] exchangePlan,
            tech.streamfusion.flink.exchange.NativeExchangeFrame frame,
            java.util.function.LongConsumer inputRows) {
        Objects.checkIndex(port, emptyInputs.size());
        if (emptyInputs.stream().anyMatch(input -> input.size() != 0))
            throw new IllegalArgumentException("Native exchange invocation requires empty input placeholders");
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(inputRows, "inputRows");
        return execute(
                emptyInputs,
                (arrays, schemas) -> frame.executeNativeRegion(context, port, exchangePlan, arrays, schemas, inputRows),
                port,
                inputEdge.samplesClock(port) ? frame.logicalRowCount() : 0);
    }

    private ArrowNativeRegionOutput execute(List<ArrowRowDataBatch> inputs, byte[] controls) {
        return execute(inputs, (arrays, schemas) -> NativeRegionStream.open(context, arrays, schemas, controls));
    }

    @FunctionalInterface
    private interface Invocation {
        NativeRegionStream open(long[] arrays, long[] schemas);
    }

    private ArrowNativeRegionOutput execute(List<ArrowRowDataBatch> inputs, Invocation invocation) {
        return execute(inputs, invocation, -1, 0);
    }

    private ArrowNativeRegionOutput execute(
            List<ArrowRowDataBatch> inputs, Invocation invocation, int decodedPort, int decodedRows) {
        try (var prepared = inputEdge.prepare(inputs, decodedPort, decodedRows)) {
            var stream = invocation.open(prepared.arrayAddresses, prepared.schemaAddresses);
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
