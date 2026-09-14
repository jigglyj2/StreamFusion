/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

/** Controllable storage finalization and reads for concurrent checkpoint lifecycle tests. */
final class NativeCheckpointTransferTestStorage extends MemCheckpointStreamFactory {
    final CountDownLatch finalized = new CountDownLatch(2);
    final CountDownLatch release = new CountDownLatch(1);
    final java.util.Queue<Handle> handles = new java.util.concurrent.ConcurrentLinkedQueue<>();
    final AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger(), reused = new AtomicInteger();
    volatile ReadGate readGate;
    int reportedOverhead;

    NativeCheckpointTransferTestStorage() {
        super(1 << 20);
    }

    @Override
    public boolean couldReuseStateHandle(StreamStateHandle handle) {
        return true;
    }

    @Override
    public void reusePreviousStateHandle(java.util.Collection<? extends StreamStateHandle> files) {
        reused.addAndGet(files.size());
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope) {
        peak.accumulateAndGet(active.incrementAndGet(), Math::max);
        return new CheckpointStateOutputStream() {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            boolean closed;

            @Override
            public synchronized void write(int value) throws IOException {
                if (closed) throw new IOException("closed");
                bytes.write(value);
            }

            @Override
            public long getPos() {
                return bytes.size();
            }

            @Override
            public void flush() {}

            @Override
            public void sync() {}

            @Override
            public StreamStateHandle closeAndGetHandle() throws IOException {
                Handle handle;
                synchronized (this) {
                    if (closed) throw new IOException("closed");
                    closed = true;
                    handle = new Handle(bytes.toByteArray(), NativeCheckpointTransferTestStorage.this);
                    handles.add(handle);
                }
                finalized.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        release.await();
                        break;
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
                if (interrupted) Thread.currentThread().interrupt();
                active.decrementAndGet();
                return handle;
            }

            @Override
            public synchronized void close() {
                if (!closed) {
                    closed = true;
                    active.decrementAndGet();
                }
            }
        };
    }

    static final class ReadGate {
        final CountDownLatch entered = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
    }

    static final class Handle extends ByteStreamStateHandle {
        final AtomicInteger discards = new AtomicInteger();
        final NativeCheckpointTransferTestStorage factory;

        Handle(byte[] bytes, NativeCheckpointTransferTestStorage factory) {
            super(UUID.randomUUID().toString(), bytes);
            this.factory = factory;
        }

        @Override
        public long getStateSize() {
            return super.getStateSize() + factory.reportedOverhead;
        }

        @Override
        public void discardState() {
            discards.incrementAndGet();
        }

        @Override
        public org.apache.flink.core.fs.FSDataInputStream openInputStream() throws IOException {
            var delegate = super.openInputStream();
            ReadGate gate = factory.readGate;
            if (gate == null) return delegate;
            return new org.apache.flink.core.fs.FSDataInputStream() {
                volatile boolean closed;
                boolean entered;

                private void gate() throws IOException {
                    if (!entered) {
                        entered = true;
                        gate.entered.countDown();
                    }
                    try {
                        if (!gate.release.await(5, TimeUnit.SECONDS)) throw new IOException("read timeout");
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IOException(failure);
                    }
                    if (closed) throw new IOException("cancelled read");
                }

                @Override
                public int read() throws IOException {
                    gate();
                    return delegate.read();
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    gate();
                    return delegate.read(bytes, offset, length);
                }

                @Override
                public void seek(long position) throws IOException {
                    delegate.seek(position);
                }

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public void close() throws IOException {
                    closed = true;
                    gate.release.countDown();
                    delegate.close();
                }
            };
        }
    }
}
