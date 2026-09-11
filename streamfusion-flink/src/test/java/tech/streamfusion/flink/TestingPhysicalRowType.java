/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/**
 * Flink BinaryWriter cannot serialize a generic DISTINCT_TYPE directly. Materialize generated
 * input with Flink's physical storage types, then retain original logical types for key selectors.
 */
public final class TestingPhysicalRowType {
    private TestingPhysicalRowType() {}

    public static RowType physicalRowType(RowType rowType) {
        List<RowType.RowField> fields = new ArrayList<>(rowType.getFieldCount());
        for (RowType.RowField field : rowType.getFields()) {
            fields.add(new RowType.RowField(
                    field.getName(),
                    physicalType(field.getType()),
                    field.getDescription().orElse(null)));
        }
        return new RowType(rowType.isNullable(), fields);
    }

    private static LogicalType physicalType(LogicalType type) {
        if (type instanceof DistinctType) {
            return physicalType(((DistinctType) type).getSourceType()).copy(type.isNullable());
        }
        if (type instanceof StructuredType) {
            List<LogicalType> types = LogicalTypeChecks.getFieldTypes(type);
            List<String> names = LogicalTypeChecks.getFieldNames(type);
            List<RowType.RowField> fields = new ArrayList<>(types.size());
            for (int index = 0; index < types.size(); index++) {
                fields.add(new RowType.RowField(names.get(index), physicalType(types.get(index))));
            }
            return new RowType(type.isNullable(), fields);
        }
        if (type instanceof ArrayType) {
            return new ArrayType(type.isNullable(), physicalType(((ArrayType) type).getElementType()));
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return new MapType(type.isNullable(), physicalType(map.getKeyType()), physicalType(map.getValueType()));
        }
        if (type instanceof MultisetType) {
            return new MultisetType(type.isNullable(), physicalType(((MultisetType) type).getElementType()));
        }
        if (type instanceof RowType) {
            return physicalRowType((RowType) type);
        }
        return type;
    }
}
