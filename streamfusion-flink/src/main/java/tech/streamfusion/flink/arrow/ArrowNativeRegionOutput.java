/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.IOUtils;
import tech.streamfusion.nativebridge.NativeRegionStream;

/** Cooperatively pulls all exits; callers route each Arrow batch using its port. */
public final class ArrowNativeRegionOutput implements AutoCloseable {
    private final NativeRegionStream stream;
    private final List<RowType> outputTypes;
    private final BufferAllocator allocator;
    private final Schema[] negotiated;
    private final Schema[] invocationSchemas;
    private final List<CDataDictionaryProvider> dictionaries = new ArrayList<>();
    private List<? extends AutoCloseable> inputEnvelopes = List.of();
    private boolean closed;
    private boolean finished;

    ArrowNativeRegionOutput(
            NativeRegionStream stream, List<RowType> types, BufferAllocator allocator, Schema[] schemas) {
        this.stream = stream;
        outputTypes = types;
        this.allocator = allocator;
        negotiated = schemas;
        invocationSchemas = new Schema[types.size()];
        // Dictionary IDs belong to each output schema, not to the whole region.
        for (int port = 0; port < types.size(); port++) dictionaries.add(new CDataDictionaryProvider());
    }

    void ownInputs(List<? extends AutoCloseable> inputs) {
        inputEnvelopes = inputs;
    }

    /** Returns null at EOF. The caller owns each returned batch independently of this stream. */
    public Batch next() {
        if (closed) throw new IllegalStateException("Native region output is closed");
        if (finished) return null;
        try (var array = ArrowArray.allocateNew(allocator);
                var schema = ArrowSchema.allocateNew(allocator)) {
            // Keep the descriptor wrappers open so finally can release unconsumed exports.
            try {
                int port = stream.next(array.memoryAddress(), schema.memoryAddress());
                if (port == -1) {
                    finished = true;
                    return null;
                }
                if (port < 0 || port >= outputTypes.size())
                    throw new IllegalStateException("Native region returned an unknown output port");
                if (invocationSchemas[port] == null) {
                    if (schema.snapshot().release == 0)
                        throw new IllegalStateException("Native region omitted its first output schema");
                    var imported = Data.importSchema(allocator, schema, dictionaries.get(port), false);
                    if (negotiated[port] != null && !negotiated[port].equals(imported))
                        throw new IllegalStateException("Arrow output schema changed after native negotiation");
                    invocationSchemas[port] = imported;
                    negotiated[port] = imported;
                } else if (schema.snapshot().release != 0) {
                    throw new IllegalStateException("Native region repeated an output schema within one invocation");
                }
                var snapshot = array.snapshot();
                if (snapshot.release == 0 || snapshot.length < 0 || snapshot.length > Integer.MAX_VALUE)
                    throw new IllegalStateException("Native region returned an invalid Arrow batch");
                var root = VectorSchemaRoot.create(invocationSchemas[port], allocator);
                try {
                    Data.importIntoVectorSchemaRoot(allocator, array, root, dictionaries.get(port), false);
                    root.setRowCount((int) snapshot.length);
                    if (root.getFieldVectors().size() != outputTypes.get(port).getFieldCount() + 3)
                        throw new IllegalStateException("Native region requires an owned output envelope");
                } catch (RuntimeException | Error failure) {
                    root.close();
                    throw failure;
                }
                return new Batch(
                        port,
                        NativePlanOutputEnvelope.read(root, outputTypes.get(port), allocator)
                                .batch());
            } finally {
                try {
                    if (array.snapshot().release != 0) array.release();
                } finally {
                    if (schema.snapshot().release != 0) schema.release();
                }
            }
        } catch (RuntimeException | Error failure) {
            try {
                close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            IOUtils.closeAll(stream, () -> IOUtils.closeAll(dictionaries), () -> IOUtils.closeAll(inputEnvelopes));
        } catch (Exception failure) {
            throw new IllegalStateException("Failed to release native region Arrow resources", failure);
        } finally {
            inputEnvelopes = List.of();
        }
    }

    public static final class Batch implements AutoCloseable {
        private final int port;
        private final ArrowRowDataBatch batch;

        private Batch(int port, ArrowRowDataBatch batch) {
            this.port = port;
            this.batch = batch;
        }

        public int port() {
            return port;
        }

        public ArrowRowDataBatch batch() {
            return batch;
        }

        @Override
        public void close() {
            batch.close();
        }
    }
}
