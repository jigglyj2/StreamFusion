/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowUtils;

/** Builds the source-side Arrow exchange batch with a stable Flink record envelope. */
public final class ArrowExchangeBatch {
    public static final String ROW_KIND_COLUMN = "__streamfusion_row_kind";
    public static final String TIMESTAMP_COLUMN = "__streamfusion_stream_record_timestamp";

    private ArrowExchangeBatch() {}

    /** Adds only the Flink envelope vectors; the user-data vectors remain shared zero-copy. */
    public static EnvelopeBatch withEnvelope(ArrowRowDataBatch input, RowType rowType) {
        RowType metadataType = RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {new TinyIntType(false), new BigIntType(true)},
                new String[] {ROW_KIND_COLUMN, TIMESTAMP_COLUMN});
        VectorSchemaRoot metadata = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(metadataType), input.allocator());
        try {
            TinyIntVector rowKinds = (TinyIntVector) metadata.getVector(0);
            BigIntVector timestamps = (BigIntVector) metadata.getVector(1);
            rowKinds.allocateNew(input.size());
            timestamps.allocateNew(input.size());
            for (int row = 0; row < input.size(); row++) {
                rowKinds.setSafe(row, input.rowKind(row).toByteValue());
                if (input.hasTimestamp(row)) {
                    timestamps.setSafe(row, input.timestamp(row));
                } else {
                    timestamps.setNull(row);
                }
            }
            rowKinds.setValueCount(input.size());
            timestamps.setValueCount(input.size());
            metadata.setRowCount(input.size());

            List<FieldVector> vectors = new ArrayList<>(input.root().getFieldVectors());
            vectors.addAll(metadata.getFieldVectors());
            VectorSchemaRoot combined = new VectorSchemaRoot(vectors);
            combined.setRowCount(input.size());
            ArrowRowDataBatch exchangeBatch =
                    ArrowRowDataBatch.borrowed(combined, exchangeRowType(rowType), input.allocator());
            return new EnvelopeBatch(exchangeBatch, metadata);
        } catch (RuntimeException | Error failure) {
            metadata.close();
            throw failure;
        }
    }

    public static RowType exchangeRowType(RowType rowType) {
        List<RowType.RowField> fields = new ArrayList<>(rowType.getFields());
        fields.add(new RowType.RowField(ROW_KIND_COLUMN, new TinyIntType(false)));
        fields.add(new RowType.RowField(TIMESTAMP_COLUMN, new BigIntType(true)));
        return new RowType(rowType.isNullable(), fields);
    }

    /** Owns the two envelope vectors, never the shared input data vectors. */
    public static final class EnvelopeBatch implements AutoCloseable {
        private final ArrowRowDataBatch batch;
        private final VectorSchemaRoot metadata;

        private EnvelopeBatch(ArrowRowDataBatch batch, VectorSchemaRoot metadata) {
            this.batch = batch;
            this.metadata = metadata;
        }

        public ArrowRowDataBatch batch() {
            return batch;
        }

        @Override
        public void close() {
            metadata.close();
        }
    }
}
