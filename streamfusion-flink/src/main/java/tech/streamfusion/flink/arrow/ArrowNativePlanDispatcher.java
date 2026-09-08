/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeExecutionContext;

/**
 * Shared arrival-driven edge for native regions whose outputs select input record envelopes.
 *
 * <p>Flink owns scheduling and control events. Each arrival runs to completion before returning to
 * its mailbox: other input ports are typed empty Arrow batches, not buffered rows. Explicit controls
 * use empty ports and independently owned output envelopes. Output callbacks borrow one bounded
 * native batch at a time; Flink's control owner must supply the correct coalesced stage events.
 */
public final class ArrowNativePlanDispatcher implements AutoCloseable {
    private final ArrowNativePlanBridge bridge;
    private final List<RowType> inputTypes;
    private final List<ArrowRowDataBatch> emptyInputs = new ArrayList<>();
    private final List<ArrowRowDataBatch> inputs = new ArrayList<>();
    private boolean processing;
    private boolean closed;

    public ArrowNativePlanDispatcher(
            NativeExecutionContext context, List<RowType> inputTypes, RowType outputType, BufferAllocator allocator) {
        this.inputTypes = List.copyOf(inputTypes);
        if (inputTypes.isEmpty()) {
            throw new IllegalArgumentException("An arrival-driven native region needs at least one input");
        }
        bridge = new ArrowNativePlanBridge(context, outputType, allocator);
        try {
            for (RowType type : this.inputTypes) {
                emptyInputs.add(ArrowRowDataBatch.empty(type, allocator));
            }
            inputs.addAll(emptyInputs);
        } catch (RuntimeException | Error failure) {
            try {
                close();
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    /** Borrows the input and each callback output only for this call; never retains an arrival. */
    public void process(int inputIndex, ArrowRowDataBatch input, Consumer<ArrowRowDataBatch> output) {
        if (closed || processing) {
            throw new IllegalStateException("Native input dispatcher is closed or already processing an arrival");
        }
        Objects.checkIndex(inputIndex, inputs.size());
        Objects.requireNonNull(output, "output");
        if (!inputTypes.get(inputIndex).equals(input.rowType())) {
            throw new IllegalArgumentException("Native input type changed at port " + inputIndex + ": expected "
                    + inputTypes.get(inputIndex) + ", received " + input.rowType());
        }
        processing = true;
        inputs.set(inputIndex, input);
        try (ArrowCDataBridge.NativeOutputStream stream = bridge.executeStream(inputs)) {
            if (input.hasTrivialEnvelope()) {
                ArrowRowDataBatch next;
                while ((next = stream.next()) != null) {
                    try (ArrowRowDataBatch batch = next) {
                        if (batch.size() != 0) {
                            output.accept(batch);
                        }
                    }
                }
            } else {
                NativeCalcResult next;
                while ((next = stream.nextWithSelection()) != null) {
                    try (NativeCalcResult result = next) {
                        // Every other port is empty, so global ordinals are local to this arrival.
                        if (result.batch().size() != 0) {
                            output.accept(result.selectEnvelopeFrom(input));
                        }
                    }
                }
            }
        } finally {
            inputs.set(inputIndex, emptyInputs.get(inputIndex));
            processing = false;
        }
    }

    /** Network frames remain native until final outputs cross the region boundary. */
    public void processFrame(
            int inputIndex,
            byte[] exchangePlan,
            tech.streamfusion.flink.exchange.NativeExchangeFrame frame,
            java.util.function.LongConsumer inputRows,
            Consumer<ArrowRowDataBatch> output) {
        if (closed || processing) throw new IllegalStateException("Native dispatcher is closed or processing");
        Objects.checkIndex(inputIndex, inputTypes.size());
        processing = true;
        try (var stream = bridge.executeExchangeStream(emptyInputs, inputIndex, exchangePlan, frame, inputRows)) {
            NativeCalcResult next;
            while ((next = stream.nextWithSelection()) != null) {
                try (NativeCalcResult result = next) {
                    if (result.batch().size() != 0) output.accept(result.selectEnvelopeFrom(emptyInputs));
                }
            }
        } finally {
            processing = false;
        }
    }

    @Override
    public void close() {
        if (processing) {
            throw new IllegalStateException("Cannot close a native input dispatcher during an arrival");
        }
        if (!closed) {
            closed = true;
            try {
                org.apache.flink.util.IOUtils.closeAll(emptyInputs);
            } catch (Exception failure) {
                throw new IllegalStateException("Could not release native input port batches", failure);
            } finally {
                inputs.clear();
                emptyInputs.clear();
            }
        }
    }

    /** Runs only caller-supplied stage events; ordinary arrival EOF never implies a flush. */
    public void control(byte[] controls, Consumer<ArrowRowDataBatch> output) {
        if (closed || processing) {
            throw new IllegalStateException("Native input dispatcher is closed or already processing an invocation");
        }
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(controls, "controls");
        processing = true;
        try (var stream = bridge.executeControlStream(emptyInputs, controls)) {
            NativeCalcResult next;
            while ((next = stream.nextWithSelection()) != null) {
                try (NativeCalcResult result = next) {
                    if (result.batch().size() != 0) output.accept(result.selectEnvelopeFrom(emptyInputs));
                }
            }
        } finally {
            processing = false;
        }
    }
}
