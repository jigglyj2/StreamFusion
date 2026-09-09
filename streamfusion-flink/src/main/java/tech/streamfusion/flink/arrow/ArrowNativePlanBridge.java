/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.List;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Reusable plan-level Arrow edge, independent of operator families and input arity. */
public final class ArrowNativePlanBridge {
    private final NativeExecutionContext context;
    private final RowType outputType;
    private final BufferAllocator allocator;
    private final NativePlanInputs inputEdge;
    private Schema outputSchema;

    @FunctionalInterface
    private interface ExchangeInvocation {
        void execute(long[] arrays, long[] schemas, long output);
    }

    public ArrowCDataBridge.NativeOutputStream executeExchangeStream(
            List<ArrowRowDataBatch> emptyInputs,
            int port,
            byte[] exchangePlan,
            tech.streamfusion.flink.exchange.NativeExchangeFrame frame,
            java.util.function.LongConsumer inputRows) {
        if (!context.requiresInputEnvelope())
            throw new IllegalStateException("IPC regions require owned input envelopes");
        return executeStream(
                emptyInputs,
                null,
                (arrays, schemas, output) ->
                        inputRows.accept(frame.executeNativePlan(context, port, exchangePlan, arrays, schemas, output)),
                port,
                inputEdge.samplesClock(port) ? frame.logicalRowCount() : 0);
    }

    public ArrowNativePlanBridge(NativeExecutionContext context, RowType outputType, BufferAllocator allocator) {
        this(context, outputType, allocator, List.of(), null);
    }

    ArrowNativePlanBridge(
            NativeExecutionContext context,
            RowType outputType,
            BufferAllocator allocator,
            List<Integer> clockPorts,
            java.util.function.LongSupplier clock) {
        this.context = context;
        this.outputType = outputType;
        this.allocator = allocator;
        inputEdge = new NativePlanInputs(context.requiresInputEnvelope(), clockPorts, clock);
    }

    public ArrowCDataBridge.NativeOutputStream executeStream(List<ArrowRowDataBatch> inputs) {
        return executeStream(inputs, null);
    }

    public ArrowCDataBridge.NativeOutputStream executeControlStream(List<ArrowRowDataBatch> inputs, byte[] controls) {
        java.util.Objects.requireNonNull(controls, "controls");
        if (inputs.stream().anyMatch(input -> input.size() != 0)) {
            throw new IllegalArgumentException("Native control invocation requires empty input batches");
        }
        return executeStream(inputs, controls);
    }

    private ArrowCDataBridge.NativeOutputStream executeStream(List<ArrowRowDataBatch> inputs, byte[] controls) {
        return executeStream(inputs, controls, null);
    }

    private ArrowCDataBridge.NativeOutputStream executeStream(
            List<ArrowRowDataBatch> inputs, byte[] controls, ExchangeInvocation exchange) {
        return executeStream(inputs, controls, exchange, -1, 0);
    }

    private ArrowCDataBridge.NativeOutputStream executeStream(
            List<ArrowRowDataBatch> inputs,
            byte[] controls,
            ExchangeInvocation exchange,
            int decodedPort,
            int decodedRows) {
        try (var prepared = inputEdge.prepare(inputs, decodedPort, decodedRows)) {
            ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
            try {
                if (exchange != null)
                    exchange.execute(prepared.arrayAddresses, prepared.schemaAddresses, stream.memoryAddress());
                else if (controls == null)
                    context.executeArrowStream(
                            prepared.arrayAddresses, prepared.schemaAddresses, stream.memoryAddress());
                else
                    context.executeArrowControlStream(
                            prepared.arrayAddresses, prepared.schemaAddresses, controls, stream.memoryAddress());
                var output = new ArrowCDataBridge.NativeOutputStream(stream, outputType, allocator, outputSchema);
                output.ownInputs(prepared.transferToOutput());
                outputSchema = output.schema();
                return output;
            } catch (RuntimeException | Error failure) {
                ArrowCDataBridge.releaseStream(stream);
                throw failure;
            }
        }
    }
}
