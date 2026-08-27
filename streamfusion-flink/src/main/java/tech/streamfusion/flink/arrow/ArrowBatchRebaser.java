/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

/** Normalizes a sliced Arrow batch to vectors whose Java-visible row index starts at zero. */
public final class ArrowBatchRebaser {
    private ArrowBatchRebaser() {}

    public static VectorSchemaRoot rebase(VectorSchemaRoot input, int offset, int length, BufferAllocator allocator) {
        if (offset < 0 || length < 0 || offset + length > input.getRowCount()) {
            throw new IndexOutOfBoundsException(
                    "Arrow slice [" + offset + ", " + (offset + length) + ") outside " + input.getRowCount());
        }
        List<FieldVector> vectors = new ArrayList<>(input.getFieldVectors().size());
        try {
            for (FieldVector inputVector : input.getFieldVectors()) {
                TransferPair transfer = inputVector.getTransferPair(allocator);
                transfer.splitAndTransfer(offset, length);
                vectors.add((FieldVector) transfer.getTo());
            }
            return new VectorSchemaRoot(input.getSchema().getFields(), vectors, length);
        } catch (RuntimeException | Error failure) {
            vectors.forEach(FieldVector::close);
            throw failure;
        }
    }
}
