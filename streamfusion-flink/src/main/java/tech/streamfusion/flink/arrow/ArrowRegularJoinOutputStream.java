/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.exchange.ArrowExchangeInputBatch;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeRegularJoinBridge;

/** One native input invocation, followed by bounded Arrow C Stream pulls at the plan edge. */
public final class ArrowRegularJoinOutputStream implements AutoCloseable {
    private final ArrowArrayStream stream;
    private final BufferAllocator allocator;
    private final NativeMemoryManager memory;
    private final RowType outputType;
    private final CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
    private Schema schema;
    private boolean closed;

    public ArrowRegularJoinOutputStream(
            long handle,
            int side,
            ArrowExchangeInputBatch input,
            List<byte[]> preencodedKeys,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memory) {
        this.allocator = allocator;
        this.memory = memory;
        this.outputType = outputType;
        stream = ArrowArrayStream.allocateNew(allocator);
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                VarBinaryVector keys = preencodedKeys == null
                        ? null
                        : ArrowRegularJoinCDataBridge.preencodedKeys(allocator, input.size(), preencodedKeys)) {
            VectorSchemaRoot exported = input.transportRoot();
            if (keys != null) {
                List<FieldVector> vectors = new ArrayList<>(exported.getFieldVectors());
                vectors.add(keys);
                exported = new VectorSchemaRoot(vectors);
                exported.setRowCount(input.size());
            }
            Data.exportVectorSchemaRoot(allocator, exported, null, array, inputSchema);
            NativeRegularJoinBridge.processStream(
                    handle, side, array.memoryAddress(), inputSchema.memoryAddress(), stream.memoryAddress());
            stream.getSchema(outputSchema);
            schema = Data.importSchema(allocator, outputSchema, dictionaries);
        } catch (IOException error) {
            close();
            throw new IllegalStateException("Failed to import regular join stream schema", error);
        } catch (RuntimeException | Error error) {
            close();
            throw error;
        }
    }

    public ArrowRowDataBatch next() {
        if (closed) {
            throw new IllegalStateException("Regular join output stream is closed");
        }
        try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
            stream.getNext(array);
            ArrowArray.Snapshot snapshot = array.snapshot();
            if (snapshot.release == 0) {
                return null;
            }
            if (snapshot.length < 0 || snapshot.length > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native regular join returned invalid stream row count");
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            try {
                Data.importIntoVectorSchemaRoot(allocator, array, root, dictionaries);
                root.setRowCount((int) snapshot.length);
            } catch (RuntimeException | Error error) {
                root.close();
                throw error;
            }
            return ArrowRegularJoinCDataBridge.removeMetadata(root, outputType, allocator, false);
        } catch (IOException error) {
            throw new IllegalStateException("Failed to drain regular join output stream", error);
        } finally {
            memory.finishArrowTransfer();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (stream.snapshot().release != 0) {
                    stream.release();
                }
            } finally {
                stream.close();
                dictionaries.close();
            }
        }
    }
}
