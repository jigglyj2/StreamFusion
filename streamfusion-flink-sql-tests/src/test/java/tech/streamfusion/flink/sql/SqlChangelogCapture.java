/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Typed Flink serialization; relation comparison must explicitly opt out of arrival ordering. */
final class SqlChangelogCapture {
    enum Order {
        CHANGELOG,
        UNORDERED_INSERTS
    }

    private SqlChangelogCapture() {}

    static byte[] encode(DataType type, Iterator<Row> rows, Order order) throws Exception {
        var converter = DataStructureConverters.getConverter(type);
        converter.open(Thread.currentThread().getContextClassLoader());
        var serializer = new RowDataSerializer((RowType) type.getLogicalType());
        var encoded = new ArrayList<byte[]>();
        var rowBytes = new DataOutputSerializer(128);
        while (rows.hasNext()) {
            Row row = rows.next();
            if (order == Order.UNORDERED_INSERTS && row.getKind() != RowKind.INSERT)
                throw new IllegalArgumentException("Unordered relation comparison cannot discard changelog order");
            RowData internal = (RowData) converter.toInternal(row);
            rowBytes.clear();
            serializer.serialize(internal, rowBytes);
            encoded.add(rowBytes.getCopyOfBuffer());
        }
        if (order == Order.UNORDERED_INSERTS) encoded.sort(Arrays::compareUnsigned);
        var result = new DataOutputSerializer(128);
        for (byte[] row : encoded) {
            result.writeInt(row.length);
            result.write(row);
        }
        return result.getCopyOfBuffer();
    }
}
