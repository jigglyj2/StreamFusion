/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/** Arrow C Stream reader over the existing Flink CSV source boundary. */
final class CsvLookupSnapshotReader extends ArrowReader {
    private final CsvLookupSnapshotSource.Cursor cursor;
    private final Schema schema;
    private boolean closed;

    CsvLookupSnapshotReader(
            CsvLookupSnapshotSource source, BufferAllocator allocator, ClassLoader loader, int batchRows)
            throws Exception {
        super(allocator);
        schema = ArrowUtils.toArrowSchema(source.rowType());
        cursor = source.open(allocator, loader, batchRows);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        if (closed) throw new IOException("CSV snapshot reader is closed");
        prepareLoadNextBatch();
        try (ArrowRowDataBatch batch = cursor.nextBatch()) {
            if (batch == null) return false;
            // VectorUnloader/Loader retain the same buffers. Every C export has its own
            // release owner, so loading the next batch never mutates a retained snapshot.
            loadRecordBatch(new VectorUnloader(batch.root()).getRecordBatch());
            return true;
        }
    }

    @Override
    protected Schema readSchema() {
        return schema;
    }

    @Override
    public long bytesRead() {
        return 0;
    } // No serialized Arrow read channel exists here.

    @Override
    protected void closeReadSource() throws IOException {
        cursor.close();
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            super.close(false);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                cursor.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        cursor.close();
    }
}
