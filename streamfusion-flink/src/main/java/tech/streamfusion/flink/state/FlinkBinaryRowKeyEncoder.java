/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Canonical Flink BinaryRow key encoding used for types Rust cannot encode identically. */
public final class FlinkBinaryRowKeyEncoder {
    private FlinkBinaryRowKeyEncoder() {}

    public static List<byte[]> encode(ArrowRowDataBatch input, RowDataKeySelector keySelector, String operatorName)
            throws Exception {
        List<byte[]> keys = new ArrayList<>(input.size());
        for (int row = 0; row < input.size(); row++) {
            RowData selected = keySelector.getKey(input.rowView(row));
            if (!(selected instanceof BinaryRowData)) {
                throw new IllegalStateException(
                        "Native " + operatorName + " requires Flink's BinaryRowData key selector");
            }
            BinaryRowData binary = (BinaryRowData) selected;
            keys.add(BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        }
        return keys;
    }

    public static boolean requiresPreencoding(RowType rowType, int[] keyFields) {
        for (int key : keyFields) {
            switch (rowType.getTypeAt(key).getTypeRoot()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                    break;
                default:
                    return true;
            }
        }
        return false;
    }
}
