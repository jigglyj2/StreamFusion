/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/** One arrival driver for tree or shared-region edges; computation stays in the native plan. */
public final class ArrowNativeRegionDispatcher implements AutoCloseable {
    private final ArrowNativePlanDispatcher tree;
    private final ArrowNativeRegionBridge shared;
    private final List<RowType> inputTypes;
    private final List<ArrowRowDataBatch> emptyInputs = new ArrayList<>();
    private final List<ArrowRowDataBatch> inputs = new ArrayList<>();
    private boolean processing;
    private boolean closed;

    public ArrowNativeRegionDispatcher(
            NativeExecutionContext context, List<RowType> inputs, List<RowType> outputs, BufferAllocator allocator) {
        this(context, inputs, outputs, allocator, List.of(), null);
    }

    public ArrowNativeRegionDispatcher(
            NativeExecutionContext context,
            List<RowType> inputs,
            List<RowType> outputs,
            BufferAllocator allocator,
            List<Integer> clockPorts,
            java.util.function.LongSupplier clock) {
        inputTypes = List.copyOf(inputs);
        if (inputs.isEmpty()) throw new IllegalArgumentException("Native region requires external inputs");
        if (!context.hasRegionOutputs()) {
            if (outputs.size() != 1) throw new IllegalArgumentException("Native tree requires one output");
            tree = new ArrowNativePlanDispatcher(context, inputs, outputs.get(0), allocator, clockPorts, clock);
            shared = null;
        } else {
            tree = null;
            shared = new ArrowNativeRegionBridge(context, outputs, allocator, clockPorts, clock);
            try {
                for (var type : inputTypes) emptyInputs.add(ArrowRowDataBatch.empty(type, allocator));
                this.inputs.addAll(emptyInputs);
            } catch (RuntimeException | Error failure) {
                try {
                    close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
    }

    public void process(int port, ArrowRowDataBatch input, BiConsumer<Integer, ArrowRowDataBatch> output) {
        Objects.requireNonNull(output, "output");
        if (tree != null) {
            tree.process(port, input, batch -> output.accept(0, batch));
            return;
        }
        requireIdle();
        Objects.checkIndex(port, inputTypes.size());
        if (!inputTypes.get(port).equals(input.rowType()))
            throw new IllegalArgumentException("Native input type changed at port " + port);
        inputs.set(port, input);
        try {
            invoke(() -> shared.executeStream(inputs), output);
        } finally {
            inputs.set(port, emptyInputs.get(port));
        }
    }

    public void processFrame(
            int port,
            byte[] plan,
            NativeExchangeFrame frame,
            LongConsumer rows,
            BiConsumer<Integer, ArrowRowDataBatch> output) {
        Objects.requireNonNull(output, "output");
        if (tree != null) {
            tree.processFrame(port, plan, frame, rows, batch -> output.accept(0, batch));
            return;
        }
        invoke(() -> shared.executeExchangeStream(emptyInputs, port, plan, frame, rows), output);
    }

    public void control(byte[] controls, BiConsumer<Integer, ArrowRowDataBatch> output) {
        Objects.requireNonNull(output, "output");
        if (tree != null) {
            tree.control(controls, batch -> output.accept(0, batch));
            return;
        }
        invoke(() -> shared.executeControlStream(emptyInputs, controls), output);
    }

    private void invoke(Supplier<ArrowNativeRegionOutput> invocation, BiConsumer<Integer, ArrowRowDataBatch> output) {
        requireIdle();
        Objects.requireNonNull(output, "output");
        processing = true;
        try (var stream = invocation.get()) {
            ArrowNativeRegionOutput.Batch next;
            while ((next = stream.next()) != null) {
                try (var value = next) {
                    if (value.batch().size() != 0) output.accept(value.port(), value.batch());
                }
            }
        } finally {
            processing = false;
        }
    }

    private void requireIdle() {
        if (closed || processing) throw new IllegalStateException("Native dispatcher is closed or processing");
    }

    @Override
    public void close() {
        if (processing) throw new IllegalStateException("Cannot close a native dispatcher during an invocation");
        if (closed) return;
        closed = true;
        try {
            org.apache.flink.util.IOUtils.closeAll(tree, () -> org.apache.flink.util.IOUtils.closeAll(emptyInputs));
        } catch (Exception failure) {
            throw new IllegalStateException("Could not release native region inputs", failure);
        } finally {
            inputs.clear();
            emptyInputs.clear();
        }
    }
}
