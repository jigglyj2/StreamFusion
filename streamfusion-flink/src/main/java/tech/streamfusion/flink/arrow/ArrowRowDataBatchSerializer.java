/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Chained-operator serializer for owned Arrow batches.
 *
 * <p>Raw Arrow batches must never cross a Flink network edge. StreamFusion exchanges encode them
 * as {@code NativeExchangeFrame}; reaching serialization here is an architectural error.
 */
public final class ArrowRowDataBatchSerializer extends TypeSerializerSingleton<ArrowRowDataBatch> {
    private static final long serialVersionUID = 1L;
    public static final ArrowRowDataBatchSerializer INSTANCE = new ArrowRowDataBatchSerializer();

    private ArrowRowDataBatchSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public ArrowRowDataBatch createInstance() {
        return null;
    }

    @Override
    public ArrowRowDataBatch copy(ArrowRowDataBatch from) {
        return from;
    }

    @Override
    public ArrowRowDataBatch copy(ArrowRowDataBatch from, ArrowRowDataBatch reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ArrowRowDataBatch record, DataOutputView target) throws IOException {
        throw networkBoundaryError();
    }

    @Override
    public ArrowRowDataBatch deserialize(DataInputView source) throws IOException {
        throw networkBoundaryError();
    }

    @Override
    public ArrowRowDataBatch deserialize(ArrowRowDataBatch reuse, DataInputView source) throws IOException {
        throw networkBoundaryError();
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw networkBoundaryError();
    }

    @Override
    public TypeSerializerSnapshot<ArrowRowDataBatch> snapshotConfiguration() {
        return new Snapshot();
    }

    private static IOException networkBoundaryError() {
        return new IOException(
                "A raw StreamFusion Arrow batch reached a Flink network edge; use the native exchange frame boundary");
    }

    public static final class Snapshot extends SimpleTypeSerializerSnapshot<ArrowRowDataBatch> {
        public Snapshot() {
            super(() -> INSTANCE);
        }
    }
}
