/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Reads the shared v2 state-created changelog envelope, independent of the producing node. */
final class NativePlanOutputEnvelope {
    static final String OWNED_TIMESTAMP_V1 = "__streamfusion_owned_timestamp_v1";

    private static final java.util.Set<String> INPUT_METADATA_FIELDS = java.util.Set.of(
            "__streamfusion_row_kind",
            "__streamfusion_input_row_kind",
            "__streamfusion_input_row",
            "__streamfusion_stream_record_timestamp",
            "__streamfusion_key",
            "__streamfusion_routing_key");

    /** Reserve actual envelope/routing fields, not SQL payloads such as opaque accumulators. */
    static boolean isReservedInputField(String name) {
        return name.startsWith("__streamfusion_owned_timestamp_") || INPUT_METADATA_FIELDS.contains(name);
    }

    private NativePlanOutputEnvelope() {}

    static NativeCalcResult read(VectorSchemaRoot root, RowType type, BufferAllocator allocator) {
        int fields = type.getFieldCount();
        try {
            boolean owned = root.getFieldVectors().size() == fields + 3;
            int kindIndex = fields + (owned ? 1 : 0);
            if ((!owned && root.getFieldVectors().size() != fields + 2)
                    || !(root.getVector(kindIndex) instanceof TinyIntVector)
                    || !(root.getVector(kindIndex + 1) instanceof IntVector)
                    || !root.getVector(kindIndex).getName().equals("__streamfusion_row_kind")
                    || !root.getVector(kindIndex + 1).getName().equals("__streamfusion_input_row")) {
                throw new IllegalStateException("Native v2 output must end with RowKind and input-ordinal metadata");
            }
            if (owned
                    && (!(root.getVector(fields) instanceof BigIntVector)
                            || !root.getVector(fields).getName().equals(OWNED_TIMESTAMP_V1)))
                throw new IllegalStateException("Native owned envelope requires the v1 BIGINT timestamp vector");
            var kinds = (TinyIntVector) root.getVector(kindIndex);
            var ordinals = (IntVector) root.getVector(kindIndex + 1);
            var nativeTimes = owned ? (BigIntVector) root.getVector(fields) : null;
            RowKind[] rowKinds = new RowKind[root.getRowCount()];
            int[] inputRows = new int[root.getRowCount()];
            boolean[] timestampPresent = owned ? new boolean[root.getRowCount()] : null;
            long[] timestamps = owned ? new long[root.getRowCount()] : null;
            for (int row = 0; row < rowKinds.length; row++) {
                if (kinds.isNull(row) || ordinals.isNull(row)) {
                    throw new IllegalStateException("Native output envelope cannot contain null metadata");
                }
                rowKinds[row] = RowKind.fromByteValue(kinds.get(row));
                inputRows[row] = ordinals.get(row);
                if (owned) {
                    if (inputRows[row] != -1)
                        throw new IllegalStateException("Native owned envelope must use detached ordinal -1");
                    timestampPresent[row] = !nativeTimes.isNull(row);
                    timestamps[row] = timestampPresent[row] ? nativeTimes.get(row) : 0;
                } else if (inputRows[row] < 0) {
                    throw new IllegalStateException("Native borrowed envelope has a negative input ordinal");
                }
            }
            var visible = root.removeVector(kindIndex + 1).removeVector(kindIndex);
            ordinals.close();
            kinds.close();
            if (owned) {
                visible = visible.removeVector(fields);
                nativeTimes.close();
            }
            var batch = ArrowRowDataBatch.wrap(visible, type, allocator);
            if (owned) batch.withEnvelope(rowKinds, timestampPresent, timestamps);
            return new NativeCalcResult(batch, inputRows, rowKinds, owned);
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }
}
