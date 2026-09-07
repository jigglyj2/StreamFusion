/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** Reusable plan-level Arrow edge, independent of operator families and input arity. */
public final class ArrowNativePlanBridge {
    private final NativeExecutionContext context;
    private final RowType outputType;
    private final BufferAllocator allocator;
    private List<Schema> inputSchemas;
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
                (arrays, schemas, output) -> inputRows.accept(
                        frame.executeNativePlan(context, port, exchangePlan, arrays, schemas, output)));
    }

    public ArrowNativePlanBridge(NativeExecutionContext context, RowType outputType, BufferAllocator allocator) {
        this.context = context;
        this.outputType = outputType;
        this.allocator = allocator;
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
        if (!context.requiresInputEnvelope()) return executeRawStream(inputs, controls, exchange);
        List<tech.streamfusion.flink.exchange.ArrowExchangeBatch.EnvelopeBatch> envelopes = new ArrayList<>();
        List<ArrowRowDataBatch> nativeInputs = new ArrayList<>();
        try {
            for (ArrowRowDataBatch input : inputs) {
                if (input.size() == 0 && inputSchemas != null) {
                    nativeInputs.add(null);
                    continue;
                }
                for (var field : input.root().getSchema().getFields()) {
                    if (NativePlanOutputEnvelope.isReservedInputField(field.getName())) {
                        throw new IllegalArgumentException(
                                "Native state input payload uses reserved metadata field " + field.getName());
                    }
                }
                var envelope = tech.streamfusion.flink.exchange.ArrowExchangeBatch.withEnvelope(input, input.rowType());
                envelopes.add(envelope);
                nativeInputs.add(envelope.batch());
            }
            var stream = executeRawStream(nativeInputs, controls, exchange);
            stream.ownInputs(envelopes);
            return stream;
        } catch (RuntimeException | Error failure) {
            try {
                org.apache.flink.util.IOUtils.closeAll(envelopes);
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private ArrowCDataBridge.NativeOutputStream executeRawStream(
            List<ArrowRowDataBatch> inputs, byte[] controls, ExchangeInvocation exchange) {
        List<Schema> schemas = new ArrayList<>(inputs.size());
        for (int index = 0; index < inputs.size(); index++) {
            ArrowRowDataBatch input = inputs.get(index);
            schemas.add(input == null ? inputSchemas.get(index) : input.root().getSchema());
        }
        if (inputSchemas != null && !inputSchemas.equals(schemas)) {
            throw new IllegalStateException("Arrow input schemas or arity changed after native negotiation");
        }
        boolean negotiate = inputSchemas == null;
        List<ArrowArray> arrays = new ArrayList<>(inputs.size());
        List<ArrowSchema> schemaHandles = new ArrayList<>(inputs.size());
        try {
            long[] arrayAddresses = new long[inputs.size()];
            long[] schemaAddresses = new long[inputs.size()];
            for (int index = 0; index < inputs.size(); index++) {
                ArrowRowDataBatch input = inputs.get(index);
                // Zero addresses mean an inactive port with a previously negotiated schema.
                if (!negotiate && (input == null || input.size() == 0)) continue;
                ArrowArray array = ArrowArray.allocateNew(input.allocator());
                arrays.add(array);
                ArrowSchema schema = negotiate ? ArrowSchema.allocateNew(input.allocator()) : null;
                if (schema != null) {
                    schemaHandles.add(schema);
                    schemaAddresses[index] = schema.memoryAddress();
                }
                Data.exportVectorSchemaRoot(input.allocator(), input.root(), null, array, schema);
                arrayAddresses[index] = array.memoryAddress();
            }
            ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
            try {
                if (exchange != null) exchange.execute(arrayAddresses, schemaAddresses, stream.memoryAddress());
                else if (controls == null)
                    context.executeArrowStream(arrayAddresses, schemaAddresses, stream.memoryAddress());
                else
                    context.executeArrowControlStream(
                            arrayAddresses, schemaAddresses, controls, stream.memoryAddress());
                var output = new ArrowCDataBridge.NativeOutputStream(stream, outputType, allocator, outputSchema);
                inputSchemas = List.copyOf(schemas);
                outputSchema = output.schema();
                return output;
            } catch (RuntimeException | Error failure) {
                ArrowCDataBridge.releaseStream(stream);
                throw failure;
            }
        } finally {
            // Rust clears a consumed C handle's release pointer. On partial import or early
            // admission failure, Java is still the owner of every handle left unconsumed.
            try {
                for (ArrowArray array : arrays) {
                    try {
                        if (array.snapshot().release != 0) {
                            array.release();
                        }
                    } finally {
                        array.close();
                    }
                }
            } finally {
                for (ArrowSchema schema : schemaHandles) {
                    try {
                        if (schema.snapshot().release != 0) {
                            schema.release();
                        }
                    } finally {
                        schema.close();
                    }
                }
            }
        }
    }
}
