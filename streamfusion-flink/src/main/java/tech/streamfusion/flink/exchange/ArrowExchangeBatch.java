/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Builds the source-side Arrow exchange batch with a stable Flink record envelope. */
public final class ArrowExchangeBatch {
    public static final String ROW_KIND_COLUMN = "__streamfusion_row_kind";
    public static final String TIMESTAMP_COLUMN = "__streamfusion_stream_record_timestamp";

    private ArrowExchangeBatch() {}

    public static ArrowRowDataBatch transpose(
            List<StreamRecord<RowData>> records, RowType rowType, BufferAllocator allocator) {
        List<ExchangeRowData> rows = new ArrayList<>(records.size());
        for (StreamRecord<RowData> record : records) {
            RowData row = record.getValue();
            rows.add(new ExchangeRowData(row, row.getRowKind(), record.hasTimestamp(), record.getTimestamp()));
        }
        return ArrowRowDataBatch.transpose(rows, exchangeRowType(rowType), allocator);
    }

    public static RowType exchangeRowType(RowType rowType) {
        List<RowType.RowField> fields = new ArrayList<>(rowType.getFields());
        fields.add(new RowType.RowField(ROW_KIND_COLUMN, new TinyIntType(false)));
        fields.add(new RowType.RowField(TIMESTAMP_COLUMN, new BigIntType(true)));
        return new RowType(rowType.isNullable(), fields);
    }
}
