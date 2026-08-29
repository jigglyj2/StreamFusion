/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/** Rejects timestamp shapes whose complete Flink value domain cannot fit an Arrow physical type. */
final class StreamFusionTimestampRangeSupport {
    private StreamFusionTimestampRangeSupport() {}

    static String unsupportedReason(LogicalType type, String path) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampReason(((TimestampType) type).getPrecision(), path);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampReason(((LocalZonedTimestampType) type).getPrecision(), path);
            case ARRAY:
                return unsupportedReason(((ArrayType) type).getElementType(), path + ".element");
            case MAP:
                MapType map = (MapType) type;
                String keyReason = unsupportedReason(map.getKeyType(), path + ".key");
                return keyReason != null ? keyReason : unsupportedReason(map.getValueType(), path + ".value");
            case MULTISET:
                return unsupportedReason(((MultisetType) type).getElementType(), path + ".element");
            case ROW:
                RowType row = (RowType) type;
                for (int index = 0; index < row.getFieldCount(); index++) {
                    String reason = unsupportedReason(row.getTypeAt(index), path + ".field[" + index + "]");
                    if (reason != null) {
                        return reason;
                    }
                }
                return null;
            default:
                return null;
        }
    }

    private static String timestampReason(int precision, String path) {
        return precision <= 6
                ? null
                : path + ": TIMESTAMP precision " + precision
                        + " stays on Flink because Arrow nanosecond timestamps cannot represent Flink's complete calendar range";
    }
}
