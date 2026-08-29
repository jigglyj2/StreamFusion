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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.flink.arrow.ArrowReader;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowUtils;

/** Owns one received Arrow frame and exposes user rows plus Flink's record envelope. */
public final class ArrowExchangeInputBatch implements AutoCloseable {
    private final VectorSchemaRoot root;
    private final ArrowReader rows;
    private final TinyIntVector rowKinds;
    private final BigIntVector timestamps;
    private final ArrowRowDataBatch batch;

    public ArrowExchangeInputBatch(VectorSchemaRoot root, RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        if (root.getFieldVectors().size() != fieldCount + 2) {
            root.close();
            throw new IllegalArgumentException("Native exchange batch does not match its envelope schema");
        }
        this.root = root;
        List<FieldVector> visibleVectors =
                new ArrayList<>(root.getFieldVectors().subList(0, fieldCount));
        VectorSchemaRoot visible = new VectorSchemaRoot(visibleVectors);
        visible.setRowCount(root.getRowCount());
        this.rows = ArrowUtils.createArrowReader(visible, rowType);
        this.rowKinds = (TinyIntVector) root.getVector(fieldCount);
        this.timestamps = (BigIntVector) root.getVector(fieldCount + 1);
        RowKind[] kinds = new RowKind[root.getRowCount()];
        boolean[] timestampPresence = new boolean[root.getRowCount()];
        long[] timestampValues = new long[root.getRowCount()];
        for (int row = 0; row < root.getRowCount(); row++) {
            kinds[row] = RowKind.fromByteValue(rowKinds.get(row));
            timestampPresence[row] = !timestamps.isNull(row);
            timestampValues[row] = timestampPresence[row] ? timestamps.get(row) : Long.MIN_VALUE;
        }
        this.batch =
                ArrowRowDataBatch.borrowed(visible, rowType).withEnvelope(kinds, timestampPresence, timestampValues);
    }

    public int size() {
        return root.getRowCount();
    }

    public RowData rowView(int row) {
        RowData value = rows.read(row);
        value.setRowKind(RowKind.fromByteValue(rowKinds.get(row)));
        return value;
    }

    public ArrowRowDataBatch arrowBatch() {
        return batch;
    }

    public boolean hasTimestamp(int row) {
        return !timestamps.isNull(row);
    }

    public long timestamp(int row) {
        return timestamps.isNull(row) ? Long.MIN_VALUE : timestamps.get(row);
    }

    @Override
    public void close() {
        root.close();
    }
}
