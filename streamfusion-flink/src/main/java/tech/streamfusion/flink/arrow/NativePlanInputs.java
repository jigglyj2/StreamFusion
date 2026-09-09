/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.util.IOUtils;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;

/** Shared input negotiation and producer ownership for tree and region C Data edges. */
final class NativePlanInputs {
    private final boolean ownedEnvelope;
    private List<Schema> negotiated;
    private final List<Integer> clockPorts;
    private final java.util.function.LongSupplier clock;

    NativePlanInputs(boolean ownedEnvelope) {
        this(ownedEnvelope, List.of(), null);
    }

    NativePlanInputs(boolean ownedEnvelope, List<Integer> clockPorts, java.util.function.LongSupplier clock) {
        this.ownedEnvelope = ownedEnvelope;
        this.clockPorts = List.copyOf(clockPorts);
        this.clock = clock;
        if (!clockPorts.isEmpty()
                && (!ownedEnvelope
                        || clock == null
                        || new java.util.HashSet<>(clockPorts).size() != clockPorts.size()
                        || clockPorts.stream().anyMatch(port -> port < 0)))
            throw new IllegalArgumentException(
                    "Clock inputs require owned envelopes, a Flink clock and distinct ports");
        if (!clockPorts.equals(clockPorts.stream().sorted().collect(java.util.stream.Collectors.toList())))
            throw new IllegalArgumentException("Clock input ports must be ordered");
    }

    boolean samplesClock(int port) {
        return clockPorts.contains(port);
    }

    Prepared prepare(List<ArrowRowDataBatch> inputs) {
        return prepare(inputs, -1, 0);
    }

    Prepared prepare(List<ArrowRowDataBatch> inputs, int decodedPort, int decodedRows) {
        if (negotiated != null && inputs.size() != negotiated.size())
            throw new IllegalStateException("Arrow input schemas or arity changed after native negotiation");
        if (clockPorts.stream().anyMatch(port -> port >= inputs.size()) || decodedRows < 0)
            throw new IllegalArgumentException("Clock input port or row count is out of range");
        var prepared = new Prepared(inputs.size() + clockPorts.size());
        try {
            for (int index = 0; index < inputs.size(); index++) {
                var input = inputs.get(index);
                if (input.size() == 0 && negotiated != null) {
                    if (!ownedEnvelope && !input.root().getSchema().equals(negotiated.get(index)))
                        throw new IllegalStateException(
                                "Arrow input schemas or arity changed after native negotiation");
                    prepared.schemas.add(negotiated.get(index));
                    continue;
                }
                if (ownedEnvelope) {
                    for (var field : input.root().getSchema().getFields()) {
                        if (NativePlanOutputEnvelope.isReservedInputField(field.getName()))
                            throw new IllegalArgumentException(
                                    "Native state input payload uses reserved metadata field " + field.getName());
                    }
                    var envelope = ArrowExchangeBatch.withEnvelope(input, input.rowType());
                    prepared.owners.add(envelope);
                    input = envelope.batch();
                }
                var schema = input.root().getSchema();
                prepared.schemas.add(schema);
                if (negotiated != null && !negotiated.get(index).equals(schema))
                    throw new IllegalStateException("Arrow input schemas or arity changed after native negotiation");
                prepared.export(index, input.root(), input.allocator(), negotiated == null);
            }
            for (int slot = 0; slot < clockPorts.size(); slot++) {
                int port = clockPorts.get(slot);
                var input = inputs.get(port);
                int rows = port == decodedPort ? decodedRows : input.size();
                var samples = NativeProcessingTimeInput.capture(rows, clock, input.allocator());
                prepared.owners.add(samples);
                prepared.export(inputs.size() + slot, samples.root(), input.allocator(), true);
            }
            return prepared;
        } catch (RuntimeException | Error failure) {
            try {
                prepared.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    final class Prepared implements AutoCloseable {
        private final List<Schema> schemas = new ArrayList<>();
        private final List<AutoCloseable> handles = new ArrayList<>();
        private List<AutoCloseable> owners = new ArrayList<>();
        final long[] arrayAddresses;
        final long[] schemaAddresses;

        private Prepared(int count) {
            arrayAddresses = new long[count];
            schemaAddresses = new long[count];
        }

        private void export(
                int index,
                org.apache.arrow.vector.VectorSchemaRoot root,
                org.apache.arrow.memory.BufferAllocator allocator,
                boolean includeSchema) {
            var array = ArrowArray.allocateNew(allocator);
            handles.add(() -> {
                try {
                    if (array.snapshot().release != 0) array.release();
                } finally {
                    array.close();
                }
            });
            var schema = includeSchema ? ArrowSchema.allocateNew(allocator) : null;
            if (schema != null) {
                handles.add(() -> {
                    try {
                        if (schema.snapshot().release != 0) schema.release();
                    } finally {
                        schema.close();
                    }
                });
                schemaAddresses[index] = schema.memoryAddress();
            }
            Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
            arrayAddresses[index] = array.memoryAddress();
        }

        /** Commit only after native import succeeds; the output owner retains the input envelopes. */
        List<? extends AutoCloseable> transferToOutput() {
            negotiated = List.copyOf(schemas);
            var transferred = owners;
            owners = List.of();
            return transferred;
        }

        @Override
        public void close() {
            try {
                // Rust clears consumed release pointers. Every unconsumed handle remains Java-owned.
                IOUtils.closeAll(() -> IOUtils.closeAll(handles), () -> IOUtils.closeAll(owners));
            } catch (Exception failure) {
                throw new IllegalStateException("Failed to release native input exports", failure);
            } finally {
                handles.clear();
                owners = List.of();
            }
        }
    }
}
