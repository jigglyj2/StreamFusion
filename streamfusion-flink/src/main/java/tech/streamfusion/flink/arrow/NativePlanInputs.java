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

    NativePlanInputs(boolean ownedEnvelope) {
        this.ownedEnvelope = ownedEnvelope;
    }

    Prepared prepare(List<ArrowRowDataBatch> inputs) {
        if (negotiated != null && inputs.size() != negotiated.size())
            throw new IllegalStateException("Arrow input schemas or arity changed after native negotiation");
        var prepared = new Prepared(inputs.size());
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
                    prepared.envelopes.add(envelope);
                    input = envelope.batch();
                }
                var schema = input.root().getSchema();
                prepared.schemas.add(schema);
                if (negotiated != null && !negotiated.get(index).equals(schema))
                    throw new IllegalStateException("Arrow input schemas or arity changed after native negotiation");
                var array = ArrowArray.allocateNew(input.allocator());
                prepared.handles.add(() -> {
                    try {
                        if (array.snapshot().release != 0) array.release();
                    } finally {
                        array.close();
                    }
                });
                ArrowSchema schemaHandle = negotiated == null ? ArrowSchema.allocateNew(input.allocator()) : null;
                if (schemaHandle != null) {
                    prepared.handles.add(() -> {
                        try {
                            if (schemaHandle.snapshot().release != 0) schemaHandle.release();
                        } finally {
                            schemaHandle.close();
                        }
                    });
                    prepared.schemaAddresses[index] = schemaHandle.memoryAddress();
                }
                Data.exportVectorSchemaRoot(input.allocator(), input.root(), null, array, schemaHandle);
                prepared.arrayAddresses[index] = array.memoryAddress();
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
        private List<ArrowExchangeBatch.EnvelopeBatch> envelopes = new ArrayList<>();
        final long[] arrayAddresses;
        final long[] schemaAddresses;

        private Prepared(int count) {
            arrayAddresses = new long[count];
            schemaAddresses = new long[count];
        }

        /** Commit only after native import succeeds; the output owner retains the input envelopes. */
        List<? extends AutoCloseable> transferToOutput() {
            negotiated = List.copyOf(schemas);
            var transferred = envelopes;
            envelopes = List.of();
            return transferred;
        }

        @Override
        public void close() {
            try {
                // Rust clears consumed release pointers. Every unconsumed handle remains Java-owned.
                IOUtils.closeAll(() -> IOUtils.closeAll(handles), () -> IOUtils.closeAll(envelopes));
            } catch (Exception failure) {
                throw new IllegalStateException("Failed to release native input exports", failure);
            } finally {
                handles.clear();
                envelopes = List.of();
            }
        }
    }
}
