/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;

/** Lightweight RowData view that exposes Flink's record envelope as two trailing Arrow fields. */
final class ExchangeRowData implements RowData {
    private final RowData row;
    private final byte rowKind;
    private final boolean hasTimestamp;
    private final long timestamp;

    ExchangeRowData(RowData row, RowKind rowKind, boolean hasTimestamp, long timestamp) {
        this.row = row;
        this.rowKind = rowKind.toByteValue();
        this.hasTimestamp = hasTimestamp;
        this.timestamp = timestamp;
    }

    @Override
    public int getArity() {
        return row.getArity() + 2;
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int position) {
        return position == row.getArity() + 1 ? !hasTimestamp : position < row.getArity() && row.isNullAt(position);
    }

    @Override
    public boolean getBoolean(int position) {
        return row.getBoolean(position);
    }

    @Override
    public byte getByte(int position) {
        return position == row.getArity() ? rowKind : row.getByte(position);
    }

    @Override
    public short getShort(int position) {
        return row.getShort(position);
    }

    @Override
    public int getInt(int position) {
        return row.getInt(position);
    }

    @Override
    public long getLong(int position) {
        return position == row.getArity() + 1 ? timestamp : row.getLong(position);
    }

    @Override
    public float getFloat(int position) {
        return row.getFloat(position);
    }

    @Override
    public double getDouble(int position) {
        return row.getDouble(position);
    }

    @Override
    public StringData getString(int position) {
        return row.getString(position);
    }

    @Override
    public DecimalData getDecimal(int position, int precision, int scale) {
        return row.getDecimal(position, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int position, int precision) {
        return row.getTimestamp(position, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int position) {
        return row.getRawValue(position);
    }

    @Override
    public byte[] getBinary(int position) {
        return row.getBinary(position);
    }

    @Override
    public ArrayData getArray(int position) {
        return row.getArray(position);
    }

    @Override
    public MapData getMap(int position) {
        return row.getMap(position);
    }

    @Override
    public RowData getRow(int position, int fieldCount) {
        return row.getRow(position, fieldCount);
    }

    @Override
    public Variant getVariant(int position) {
        return row.getVariant(position);
    }
}
