/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.io.IOException;
import java.io.Serializable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.table.types.logical.RowType;

/**
 * Serializable description of a finite, immutable lookup snapshot. Each task and recovery opens
 * its own cursor and drains it before accepting probe records. Implementations retain their
 * connector's configuration and control semantics; this interface does not admit new connectors.
 */
public interface ArrowLookupSnapshotSource extends Serializable {
    RowType rowType();

    Cursor open(BufferAllocator allocator, ClassLoader loader, int batchRows) throws Exception;

    interface Cursor extends AutoCloseable {
        /** Returns null at EOF. Returned batches retain independent ownership after cursor close. */
        ArrowRowDataBatch nextBatch() throws IOException;

        @Override
        void close() throws IOException;
    }
}
