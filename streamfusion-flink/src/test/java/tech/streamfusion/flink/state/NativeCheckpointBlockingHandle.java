/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

/** A deterministic state-read cancellation gate, with no polling or timing assumptions. */
final class NativeCheckpointBlockingHandle extends ByteStreamStateHandle {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger closed = new AtomicInteger();
    final AtomicInteger opens = new AtomicInteger();
    final int blockAt;

    NativeCheckpointBlockingHandle(byte[] bytes, int blockAt) {
        super("blocking", bytes);
        this.blockAt = blockAt;
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        return Optional.empty();
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        var source = super.openInputStream();
        if (opens.incrementAndGet() != blockAt) return source;
        return new FSDataInputStream() {
            volatile boolean cancelled;

            @Override
            public void seek(long position) throws IOException {
                source.seek(position);
            }

            @Override
            public long getPos() throws IOException {
                return source.getPos();
            }

            @Override
            public int read() throws IOException {
                waitForClose();
                return source.read();
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                waitForClose();
                return source.read(bytes, offset, length);
            }

            void waitForClose() throws IOException {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("read timeout");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IOException(failure);
                }
                if (cancelled) throw new IOException("cancelled");
            }

            @Override
            public void close() throws IOException {
                cancelled = true;
                closed.incrementAndGet();
                release.countDown();
                source.close();
            }
        };
    }
}
