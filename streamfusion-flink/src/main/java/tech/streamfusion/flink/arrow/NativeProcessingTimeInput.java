/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** Per-record Flink clock samples for one receiving native edge, separate from SQL payload. */
final class NativeProcessingTimeInput implements AutoCloseable {
    static final String FIELD = "__streamfusion_processing_time_v1";
    private final VectorSchemaRoot root;

    private NativeProcessingTimeInput(VectorSchemaRoot root) {
        this.root = root;
    }

    static NativeProcessingTimeInput capture(int rows, LongSupplier clock, BufferAllocator allocator) {
        if (rows < 0) throw new IllegalArgumentException("Processing-time input row count must be non-negative");
        Objects.requireNonNull(clock, "clock");
        var field = new Field(FIELD, FieldType.notNullable(new ArrowType.Int(64, true)), List.of());
        var vector = new BigIntVector(field, allocator);
        try {
            // Admit the complete vector through the edge's Flink-backed Arrow allocator before
            // reading the clock. No JNI callback or temporary allocation per sample.
            vector.allocateNew(rows);
            for (int row = 0; row < rows; row++) vector.set(row, clock.getAsLong());
            vector.setValueCount(rows);
            return new NativeProcessingTimeInput(new VectorSchemaRoot(List.of(field), List.of(vector), rows));
        } catch (RuntimeException | Error failure) {
            vector.close();
            throw failure;
        }
    }

    VectorSchemaRoot root() {
        return root;
    }

    @Override
    public void close() {
        root.close();
    }
}
