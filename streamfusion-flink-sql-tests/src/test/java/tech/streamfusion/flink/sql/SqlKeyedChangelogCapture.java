/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/** Preserve each key's complete changelog while ignoring independent-key network interleaving. */
final class SqlKeyedChangelogCapture {
    private SqlKeyedChangelogCapture() {}

    static byte[] encode(DataType type, Iterator<Row> rows, int[] keyFields) throws Exception {
        if (keyFields.length == 0) throw new IllegalArgumentException("Keyed comparison requires explicit keys");
        var rowType = (RowType) type.getLogicalType();
        var fields = new ArrayList<RowType.RowField>();
        var getters = new RowData.FieldGetter[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            int index = java.util.Objects.checkIndex(keyFields[i], rowType.getFieldCount());
            fields.add(rowType.getFields().get(index));
            getters[i] = RowData.createFieldGetter(rowType.getTypeAt(index), index);
        }
        var converter = DataStructureConverters.getConverter(type);
        converter.open(Thread.currentThread().getContextClassLoader());
        var serializer = new RowDataSerializer(rowType);
        var keySerializer = new RowDataSerializer(new RowType(fields));
        var grouped = new TreeMap<byte[], List<byte[]>>(Arrays::compareUnsigned);
        var bytes = new DataOutputSerializer(128);
        while (rows.hasNext()) {
            var row = (RowData) converter.toInternal(rows.next());
            var key = new GenericRowData(keyFields.length);
            for (int i = 0; i < getters.length; i++) key.setField(i, getters[i].getFieldOrNull(row));
            bytes.clear();
            keySerializer.serialize(key, bytes);
            byte[] encodedKey = bytes.getCopyOfBuffer();
            bytes.clear();
            serializer.serialize(row, bytes);
            grouped.computeIfAbsent(encodedKey, ignored -> new ArrayList<>()).add(bytes.getCopyOfBuffer());
        }
        bytes.clear();
        for (var entry : grouped.entrySet()) {
            bytes.writeInt(entry.getKey().length);
            bytes.write(entry.getKey());
            bytes.writeInt(entry.getValue().size());
            for (byte[] row : entry.getValue()) {
                bytes.writeInt(row.length);
                bytes.write(row);
            }
        }
        return bytes.getCopyOfBuffer();
    }
}
