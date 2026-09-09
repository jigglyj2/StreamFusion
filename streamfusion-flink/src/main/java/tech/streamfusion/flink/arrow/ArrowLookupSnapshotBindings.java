/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;

/** Opens immutable lookup sources at the task edge, before native capability negotiation. */
public final class ArrowLookupSnapshotBindings {
    private ArrowLookupSnapshotBindings() {}

    public static NativeExecutionContext create(
            byte[] plan,
            NativeMemoryManager memory,
            byte[] state,
            byte[] task,
            boolean region,
            Map<Long, CsvLookupSnapshotSource> sources,
            BufferAllocator allocator,
            ClassLoader loader,
            int batchRows)
            throws Exception {
        if (sources.isEmpty()) throw new IllegalArgumentException("Lookup sources must not be empty");
        NativeExecutionContext context = null;
        try (Streams owned = new Streams()) {
            long[] ids = new long[sources.size()];
            long[] addresses = new long[sources.size()];
            int index = 0;
            for (var source : sources.entrySet()) {
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                owned.streams.add(stream);
                CsvLookupSnapshotReader reader =
                        new CsvLookupSnapshotReader(source.getValue(), allocator, loader, batchRows);
                try {
                    Data.exportArrayStream(allocator, reader, stream);
                } catch (RuntimeException | Error failure) {
                    try {
                        reader.close();
                    } catch (Exception cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    throw failure;
                }
                ids[index] = source.getKey();
                addresses[index++] = stream.memoryAddress();
            }
            context = NativeExecutionContext.withLookupSources(plan, memory, state, task, region, ids, addresses);
        } catch (Exception | Error failure) {
            if (context != null) {
                try {
                    context.close();
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
        return context;
    }

    private static final class Streams implements AutoCloseable {
        final List<ArrowArrayStream> streams = new ArrayList<>();

        @Override
        public void close() {
            Throwable failure = null;
            for (int i = streams.size() - 1; i >= 0; i--) {
                try {
                    ArrowCDataBridge.releaseStream(streams.get(i));
                } catch (RuntimeException | Error cleanup) {
                    if (failure == null) failure = cleanup;
                    else failure.addSuppressed(cleanup);
                }
            }
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
        }
    }
}
